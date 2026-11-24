"""Controlled shutdown on SIGINT / SIGTERM (audit L7).

The first signal asks the manager to stop safely (``CareerManager.request_cancel``): the
handler only sets an event, so it can never interrupt a database transaction. The
dispatcher stops claiming work, workers stop at their next safe point and hand their tasks
back through ownership-checked transitions, and the session ends once no worker holds
valid ownership (see ``CareerManager.run``).

A second signal forces the process to exit at once (``os._exit``), exactly like an
unexpected process death: nothing is cleaned up, every committed transaction stays
committed and nothing half-written survives (each write is one SQLite transaction), and
the session's leases simply expire, after which recovery resumes the work deterministically
(open attempts keep their numbers; in-flight calls are abandoned and reconciled if a late
reply ever arrives).

Signal handlers can only be installed from the main thread; this is used by the CLI.
Library callers can call ``request_cancel`` themselves.
"""

from __future__ import annotations

import os
import signal
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import FrameType
from typing import Protocol

DEFAULT_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class Cancellable(Protocol):
    def request_cancel(self, reason: str = ...) -> bool: ...


def forced_exit_code(signum: int) -> int:
    """Conventional exit status for death by signal (130 for SIGINT, 143 for SIGTERM)."""
    return 128 + signum


@contextmanager
def graceful_shutdown(
    target: Cancellable,
    *,
    signals: tuple[signal.Signals, ...] = DEFAULT_SIGNALS,
    notify: Callable[[str], None] | None = None,
    force_exit: Callable[[int], None] = os._exit,
) -> Iterator[None]:
    """Install SIGINT/SIGTERM handlers for the duration of a run.

    First signal: ``target.request_cancel(<signal name>)``. Any further signal: forced exit
    with ``128 + signum``. If no signal arrived, the previous handlers are restored on
    exit; after one has, the forcing handler stays installed so that a later signal (for
    example while the interpreter waits for a provider call that outlived the grace
    period) still exits immediately.
    """
    say = notify or (lambda msg: print(msg, file=sys.stderr, flush=True))
    received: list[int] = []

    def handler(signum: int, _frame: FrameType | None) -> None:
        received.append(signum)
        if len(received) == 1:
            target.request_cancel(signal.Signals(signum).name)
            say(
                f"{signal.Signals(signum).name}: stopping safely (finishing in-progress "
                "steps; press again to exit immediately)"
            )
            return
        say(f"{signal.Signals(signum).name} again: exiting immediately")
        force_exit(forced_exit_code(signum))

    previous = {s: signal.signal(s, handler) for s in signals}
    try:
        yield
    finally:
        if not received:
            for s, h in previous.items():
                signal.signal(s, h)
