#!/usr/bin/env python3
"""Rotary dial launcher.

Line N of the config file is the shell command for dial position N.
Turning the dial stops the previous command (SIGTERM, then SIGKILL after a
timeout) and starts the new one.
"""

import asyncio
import os
import signal
import sys
from pathlib import Path

CONFIG_PATH = Path(os.environ.get("DIAL_CONFIG", "dial.conf"))
GRACE_SECONDS = 5

def load_commands(path: Path) -> dict[int, str]:
    """Position (1-based) -> command. Blank lines and '#' comments are skipped
    but do NOT consume a position, so only real commands are numbered."""
    commands = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        commands[len(commands) + 1] = line
    return commands


class DialRunner:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.proc: asyncio.subprocess.Process | None = None
        self.position: int | None = None
        self._lock = asyncio.Lock()
        self._latest = 0

    async def select(self, position: int):
        self._latest += 1
        token = self._latest
        async with self._lock:
            if token != self._latest:
                return  # a newer turn arrived while we waited; skip this one
            
            try:
                commands = load_commands(self.config_path)
            except OSError as e:
                print(f"Cannot read config: {e}", file=sys.stderr)
                return

            cmd = commands.get(position)
            if cmd is None:
                print(f"No command configured for position {position}")
                return

            if position == self.position and self._running():
                return

            await self._stop()
            await self._start(position, cmd)

    def _running(self) -> bool:
        return self.proc is not None and self.proc.returncode is None

    async def _start(self, position: int, cmd: str):
        print(f"[{position}] starting: {cmd}")
        # start_new_session=True puts the command in its own process group, so
        # we can signal the shell AND everything it spawns (browsers fork a lot).
        self.proc = await asyncio.create_subprocess_exec(
            "bash", "-c", cmd,
            stdin=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        self.position = position

    async def _stop(self):
        proc = self.proc
        self.proc, self.position = None, None
        if proc is None:
            return
        pgid = proc.pid  # session leader, so pgid == pid
        if proc.returncode is None:
            print(f"stopping pid {proc.pid} (SIGTERM)")
            self._signal_group(pgid, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), GRACE_SECONDS)
            except asyncio.TimeoutError:
                print(f"pid {proc.pid} still running, SIGKILL")
                self._signal_group(pgid, signal.SIGKILL)
                await proc.wait()
        # Bash may have exited while children linger in the group; sweep them.
        self._signal_group(pgid, signal.SIGKILL)

    @staticmethod
    def _signal_group(pgid: int, sig: int):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass

    async def shutdown(self):
        async with self._lock:
            await self._stop()


async def main():
    runner = DialRunner(CONFIG_PATH)
    loop = asyncio.get_running_loop()

    stop_event = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    def on_turn(position: int):
        """Thread-safe entry point: call from GPIO callbacks."""
        loop.call_soon_threadsafe(lambda: asyncio.create_task(runner.select(position)))

    # --- GPIO wiring (one pin per position, active low). Adjust pins. ---
    # from gpiozero import Button
    # PINS = {1: 5, 2: 6, 3: 13, 4: 19, 5: 26, 6: 21}
    # buttons = []
    # for pos, pin in PINS.items():
    #     b = Button(pin, pull_up=True)
    #     b.when_pressed = lambda p=pos: on_turn(p)
    #     buttons.append(b)

    # Demo without hardware: type 1-6 + Enter on stdin.
    async def stdin_demo():
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        while line := await reader.readline():
            if line.strip().isdigit():
                on_turn(int(line))

    demo = asyncio.create_task(stdin_demo())
    await stop_event.wait()
    demo.cancel()
    await runner.shutdown()


if __name__ == "__main__":
    asyncio.run(main())