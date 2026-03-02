"""Base worker: lifecycle, heartbeat, signal handling. All workers must inherit and implement handle_signal(command)."""

import logging
import signal
import socket
import threading
import time
from abc import ABC
from typing import Any

from src.core.config import get_config
from src.core.logging import get_flight_logger
from src.models.entities import WorkerState
from src.repository.system_metadata_repo import SystemMetadataRepository
from src.repository.worker_repo import WorkerRepository

_log = logging.getLogger(__name__)


def _resolve_idle_poll_seconds() -> float:
    """
    Resolve idle poll duration from config.

    Shipping config should keep this at 5.0s or below to respect the project
    convention, but tests and local overrides are free to shorten or extend it.
    """
    try:
        cfg = get_config()
        value = getattr(cfg, "worker_idle_poll_seconds", 5.0)
    except Exception:
        value = 5.0
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        seconds = 5.0
    if seconds <= 0:
        seconds = 5.0
    return seconds


class BaseWorker(ABC):
    """
    Base for all workers. Manages run loop, heartbeat thread, and command handling.
    Subclasses must implement handle_signal(command).
    """

    REQUIRED_SCHEMA_VERSION = "1"

    def __init__(
        self,
        worker_id: str,
        repository: WorkerRepository,
        heartbeat_interval_seconds: float = 15.0,
        *,
        system_metadata_repo: SystemMetadataRepository,
    ) -> None:
        self.worker_id = worker_id
        self._repo = repository
        self._heartbeat_interval = heartbeat_interval_seconds
        self._system_metadata_repo = system_metadata_repo
        self.hostname = socket.gethostname()
        self._state = WorkerState.idle
        self._shutdown = False
        self.should_exit = False  # Alias for _shutdown; subclasses may check this
        self._heartbeat_thread: threading.Thread | None = None
        self._idle_poll_seconds: float = 5.0

    def _check_compatibility(self) -> None:
        """
        Fail-fast if schema_version in DB is missing or does not match REQUIRED_SCHEMA_VERSION.
        Raises RuntimeError when incompatible.
        """
        version = self._system_metadata_repo.get_schema_version()
        if version is None:
            raise RuntimeError(
                "Pre-flight check failed: system_metadata.schema_version is missing. "
                "Run migrations (alembic upgrade head)."
            )
        if version != self.REQUIRED_SCHEMA_VERSION:
            raise RuntimeError(
                f"Pre-flight check failed: schema_version is '{version}', "
                f"worker requires '{self.REQUIRED_SCHEMA_VERSION}'. "
                "Upgrade the database or run a compatible worker version."
            )

    def _set_state(self, new_state: WorkerState, persist: bool = True) -> None:
        """
        Update the in-memory worker state and, when persist is True, write it to the repository.
        Subclasses should prefer this helper over mutating _state and calling repo.set_state directly.
        """
        self._state = new_state
        if persist:
            self._repo.set_state(self.worker_id, new_state)

    def _handle_pause(self) -> None:
        """Transition to paused state."""
        self._set_state(WorkerState.paused)

    def _handle_resume(self) -> None:
        """Transition back to idle state."""
        self._set_state(WorkerState.idle)

    def _handle_shutdown(self) -> None:
        """Transition to offline and request run-loop exit."""
        self._shutdown = True
        self.should_exit = True
        self._set_state(WorkerState.offline)

    def handle_signal(self, command: str) -> None:
        """
        Default signal handler: supports pause, resume, shutdown, and forensic_dump.
        Subclasses may override this method, but should normally call super().handle_signal(command)
        to preserve the standard lifecycle transitions.
        """
        if command == "pause":
            self._handle_pause()
        elif command == "resume":
            self._handle_resume()
        elif command == "shutdown":
            self._handle_shutdown()

    def process_task(self) -> bool | None:
        """One unit of work. Override in subclasses; default no-op. Return True if work was done, False/None if not."""
        return None

    def get_heartbeat_stats(self) -> dict[str, Any] | None:
        """Optional stats to include in heartbeat. Override to return a dict."""
        return None

    def _heartbeat_loop(self) -> None:
        """Daemon thread: update last_seen_at and stats every heartbeat_interval_seconds."""
        while not self._shutdown:
            time.sleep(self._heartbeat_interval)
            if self._shutdown:
                break
            try:
                self._repo.update_heartbeat(
                    self.worker_id,
                    stats=self.get_heartbeat_stats(),
                )
            except Exception:  # noqa: S110
                logging.error("Heartbeat failed", exc_info=True)

    def _install_signal_handlers(self) -> None:
        """Register SIGINT and SIGTERM to set _shutdown for graceful shutdown (main thread only). Does not raise."""
        if threading.current_thread() is not threading.main_thread():
            return

        def _handler(_signum: int, _frame: Any) -> None:
            self._shutdown = True
            self.should_exit = True

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def run(self, once: bool = False) -> None:
        """
        Main entry: pre-flight compatibility check, register worker, start heartbeat thread, run loop.
        Loop checks DB for command (pause/resume/shutdown), calls handle_signal, then process_task when not paused.
        When once=True, repeatedly calls process_task() until it returns False (no work)
        or a shutdown signal is received, without entering the idle sleep/poll loop.
        Idle sleep when no work is available in normal mode is fixed at 5 seconds.
        On exit, deregisters the worker (removes row from worker_status). Exit code is 0.
        """
        self._install_signal_handlers()
        self._check_compatibility()
        self._repo.register_worker(self.worker_id, WorkerState.idle, self.hostname)
        self._state = WorkerState.idle
        self._shutdown = False
        self._idle_poll_seconds = _resolve_idle_poll_seconds()

        _log.info("Worker %s starting.", self.worker_id)

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        idle_period_started = False
        try:
            while not self._shutdown and not self.should_exit:
                cmd = self._repo.get_command(self.worker_id)
                if cmd != "none":
                    if cmd in ("pause", "resume", "shutdown"):
                        self.handle_signal(cmd)
                    elif cmd == "forensic_dump":
                        fl = get_flight_logger()
                        if fl is not None:
                            fl.dump(self.worker_id)
                    self._repo.clear_command(self.worker_id)

                if self._shutdown or self.should_exit:
                    break

                if self._state == WorkerState.paused:
                    time.sleep(1)
                    continue

                if once:
                    result = self.process_task()
                    if not result:
                        break
                    idle_period_started = False
                    _log.info("Work done.")
                    time.sleep(0.1)
                    continue

                result = self.process_task()
                if result:
                    idle_period_started = False
                    _log.info("Work done.")
                    time.sleep(0.1)
                else:
                    _log.info("Idle, sleeping %ss", self._idle_poll_seconds)
                    idle_period_started = True
                    try:
                        time.sleep(self._idle_poll_seconds)
                    except InterruptedError:
                        break
                    if self._shutdown or self.should_exit:
                        break
        finally:
            self._shutdown = True
            self.should_exit = True
            _log.info("Shutdown requested, exiting cleanly.")
            try:
                self._repo.unregister_worker(self.worker_id)
            except Exception:
                logging.error(f"Failed to unregister worker {self.worker_id} during shutdown", exc_info=True)
            if self._heartbeat_thread is not None:
                self._heartbeat_thread.join(timeout=2.0)
