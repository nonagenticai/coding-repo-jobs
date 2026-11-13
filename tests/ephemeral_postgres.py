"""Ephemeral ``postgres:18`` container for the test suite when no DSN is configured.

Ruling (George, 2026-10-21): every dev/test/CI database is an ephemeral Docker
``postgres:18``. There is NO host:5432 default -- that silently hits whatever runs on
that port on a dev box (a real DB, or another session's container).

Resolution order:

1. the explicit env var(s) the suite reads (CI exports ``DATABASE_URL``) -- honoured as-is;
2. else, if ``docker`` is usable, start ONE container for the whole test SESSION on a
   RANDOM loopback port (``-p 127.0.0.1::5432``, never 5432/8000), wait for a real TCP
   ``SELECT 1``, create the database, and export the DSN into ``os.environ``;
3. else ``pytest.exit`` with a one-line message naming the variable (only once a test
   actually needs the database).

One container per session, never one per xdist worker: ``start_session_database`` runs
in the CONTROLLER only (``pytest_configure`` without ``config.workerinput``, or conftest
import time in a process that is not an xdist worker), BEFORE xdist spawns workers, so
the workers inherit the exported DSN. A worker never starts a container; if it finds no
DSN it stops the session.

Teardown is by the container's own ID only (never by image or a broad filter): the
controller's ``pytest_unconfigure`` (``stop_session_database``), a pytest finalizer when
a fixture started it, ``atexit`` for crashes/KeyboardInterrupt, and a SIGTERM/SIGHUP
handler that removes the container and then re-delivers the signal.
"""

from __future__ import annotations

import atexit
import os
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Any

import psycopg
import pytest
from psycopg import sql

IMAGE = "postgres:18"
# Throwaway tmpfs container bound to loopback, removed at session end.
PASSWORD = "postgres"  # noqa: S105  # pragma: allowlist secret
_container_id: str | None = None
_Handler = Callable[[int, FrameType | None], Any] | int | signal.Handlers | None
_prev_handlers: dict[int, _Handler] = {}


def _is_xdist_worker(config: pytest.Config | None = None) -> bool:
    return (config is not None and hasattr(config, "workerinput")) or (
        "PYTEST_XDIST_WORKER" in os.environ
    )


def _docker_usable() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return (
            subprocess.run(
                ["docker", "info", "--format", "{{.ServerVersion}}"],
                capture_output=True,
                timeout=20,
                check=False,
            ).returncode
            == 0
        )
    except OSError, subprocess.TimeoutExpired:
        return False


def stop_container() -> None:
    """Stop (and, via ``--rm``, remove) the container THIS session started -- by its ID only."""
    global _container_id
    cid, _container_id = _container_id, None
    if cid:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, timeout=60, check=False)


def _restore_signal_handlers() -> None:
    for signum, prev in _prev_handlers.items():
        signal.signal(signum, prev if prev is not None else signal.SIG_DFL)
    _prev_handlers.clear()


def _on_signal(signum: int, frame: FrameType | None) -> None:
    """Remove the container, then let the signal do what it would have done."""
    prev = _prev_handlers.get(signum, signal.SIG_DFL)
    stop_container()
    _restore_signal_handlers()
    if not isinstance(prev, int) and prev is not None:  # a Python handler was installed
        prev(signum, frame)
    # Re-deliver with the default action so the process really terminates (rc 128+N).
    # Never sys.exit() here: pytest catches SystemExit raised inside a test as a failure
    # and the run keeps going.
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def _install_signal_handlers() -> None:
    """SIGTERM/SIGHUP skip atexit, so remove the container from a handler (controller only)."""
    if threading.current_thread() is not threading.main_thread():
        return
    for name in ("SIGTERM", "SIGHUP"):
        signum = getattr(signal, name, None)
        if signum is None or signum in _prev_handlers:
            continue
        prev = signal.getsignal(signum)
        if prev is signal.SIG_IGN:  # e.g. nohup: the run is meant to survive this signal
            continue
        _prev_handlers[signum] = prev
        signal.signal(signum, _on_signal)


def _wait_ready(port: int, deadline_s: float = 90.0) -> None:
    deadline = time.monotonic() + deadline_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host="127.0.0.1",
                port=port,
                user="postgres",
                password=PASSWORD,
                dbname="postgres",
                connect_timeout=3,
            ) as conn:
                conn.execute("SELECT 1")
            return
        except psycopg.Error as exc:  # entrypoint's temp server, or not up yet
            last = exc
            time.sleep(0.5)
    raise RuntimeError(f"ephemeral postgres on 127.0.0.1:{port} not ready in {deadline_s}s: {last}")


def start_postgres(dbname: str) -> tuple[int, str]:
    """Start the session container, create ``dbname``; return ``(port, password)``."""
    global _container_id
    if _container_id is not None:
        raise RuntimeError("ephemeral postgres already started for this session")
    _install_signal_handlers()  # before `docker run`: a signal during startup still cleans up
    atexit.register(stop_container)  # no-op until the container ID is known
    out = subprocess.run(
        [
            "docker", "run", "--rm", "-d",
            "--tmpfs", "/var/lib/postgresql",
            "-p", "127.0.0.1::5432",
            "-e", f"POSTGRES_PASSWORD={PASSWORD}",
            "--label", f"pytest-ephemeral-pg.pid={os.getpid()}",
            IMAGE,
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=True,
    )  # fmt: skip
    _container_id = out.stdout.strip()
    try:
        mapped = subprocess.run(
            ["docker", "port", _container_id, "5432/tcp"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        ).stdout
        port = int(mapped.splitlines()[0].rsplit(":", 1)[1])
        _wait_ready(port)
        if dbname != "postgres":
            with psycopg.connect(
                host="127.0.0.1",
                port=port,
                user="postgres",
                password=PASSWORD,
                dbname="postgres",
                autocommit=True,
            ) as conn:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    except BaseException:
        stop_container()
        raise
    return port, PASSWORD


def start_session_database(
    *,
    env_vars: tuple[str, ...],
    export_var: str,
    dbname: str,
    driver: str = "postgresql+psycopg",
    config: pytest.Config | None = None,
) -> str | None:
    """CONTROLLER ONLY: start the one session container and export its DSN for the workers.

    Call from ``pytest_configure`` (pass ``config``) or at conftest import time. No-op in
    an xdist worker, when a DSN is already configured, or when docker is unusable (the
    fixture then fails closed when a test needs the database). Returns the DSN or None.
    """
    if _is_xdist_worker(config):
        return None
    for name in env_vars:
        if os.environ.get(name):
            return os.environ[name]
    if not _docker_usable():
        return None
    port, password = start_postgres(dbname)
    url = f"{driver}://postgres:{password}@127.0.0.1:{port}/{dbname}"  # pragma: allowlist secret
    os.environ[export_var] = url
    return url


def stop_session_database(config: pytest.Config | None = None) -> None:
    """CONTROLLER ONLY (``pytest_unconfigure``): remove the session container by its ID."""
    if _is_xdist_worker(config):
        return
    stop_container()
    _restore_signal_handlers()


def resolve_test_database_url(
    env_vars: tuple[str, ...],
    export_var: str,
    dbname: str,
    driver: str = "postgresql+psycopg",
    request: pytest.FixtureRequest | None = None,
) -> str:
    """Return the configured DSN, else start an ephemeral container, else stop the session."""
    for name in env_vars:
        if os.environ.get(name):
            return os.environ[name]
    if _is_xdist_worker():
        pytest.exit(
            f"{' / '.join(env_vars)} is not set in this xdist worker: the controller "
            "starts the one session database and exports it before workers spawn; "
            "workers never start their own.",
            returncode=pytest.ExitCode.USAGE_ERROR,
        )
    if not _docker_usable():
        pytest.exit(
            f"{' / '.join(env_vars)} is not set and docker is unavailable; DB tests refuse "
            "to guess a DSN. Start an ephemeral DB: docker run --rm -d --tmpfs "
            "/var/lib/postgresql -p 127.0.0.1::5432 -e POSTGRES_PASSWORD=postgres "  # pragma: allowlist secret
            f"postgres:18, then export {export_var}={driver}://postgres:postgres@"  # pragma: allowlist secret
            "127.0.0.1:<port>/postgres",
            returncode=pytest.ExitCode.USAGE_ERROR,
        )
    url = start_session_database(
        env_vars=env_vars, export_var=export_var, dbname=dbname, driver=driver
    )
    if url is None:  # unreachable: not a worker, docker usable, no DSN set
        raise RuntimeError("ephemeral postgres did not start")
    if request is not None:
        request.addfinalizer(stop_container)
    return url
