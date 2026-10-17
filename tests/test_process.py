from __future__ import annotations

import asyncio
import time

import pytest

from hpc_sandbox.process import BoundedOutput, run_process
from hpc_sandbox.procfs import owned_live_start_time


async def wait_for_pid_file(path) -> int:
    while not path.exists() or not path.read_text().strip():
        await asyncio.sleep(0.02)
    return int(path.read_text())


async def gone(pid: int) -> bool:
    for _ in range(50):
        if owned_live_start_time(pid) is None:
            return True
        await asyncio.sleep(0.05)
    return False


async def test_captures_both_streams_and_the_exit_status():
    result = await run_process(["/bin/sh", "-c", "echo out; echo err >&2; exit 3"], timeout_s=10)
    assert (result.returncode, result.stdout, result.stderr) == (3, b"out\n", b"err\n")
    assert not result.timed_out


async def test_signal_deaths_use_the_shell_convention():
    result = await run_process(["/bin/sh", "-c", "kill -9 $$"], timeout_s=10)
    assert result.returncode == 137


async def test_stdin_is_piped_and_otherwise_empty():
    assert (await run_process(["cat"], timeout_s=10, stdin=b"data")).stdout == b"data"
    assert (await run_process(["cat"], timeout_s=10)).stdout == b""


async def test_timeout_kills_descendants_that_left_the_process_group(tmp_path):
    pid_file = tmp_path / "pid"
    script = f"setsid sleep 60 & echo $! > {pid_file}; echo started; wait"
    result = await run_process(["/bin/sh", "-c", script], timeout_s=1)
    assert result.timed_out and result.returncode == 124
    assert result.stdout == b"started\n"
    assert await gone(int(pid_file.read_text()))


async def test_returns_when_a_background_child_keeps_stdout_open(tmp_path):
    started = time.monotonic()
    result = await run_process(["/bin/sh", "-c", "sleep 30 & echo $!"], timeout_s=20)
    assert time.monotonic() - started < 5
    assert result.returncode == 0
    background = int(result.stdout)
    assert owned_live_start_time(background) is not None
    await run_process(["kill", str(background)], timeout_s=10)


async def test_cancellation_kills_the_process_tree(tmp_path):
    pid_file = tmp_path / "pid"
    task = asyncio.create_task(
        run_process(["/bin/sh", "-c", f"sleep 60 & echo $! > {pid_file}; wait"], timeout_s=60)
    )
    child = await wait_for_pid_file(pid_file)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await gone(child)


async def test_on_start_sees_the_process_id():
    seen: list[int] = []
    result = await run_process(["/bin/sh", "-c", "echo $$"], timeout_s=10, on_start=seen.append)
    assert seen == [int(result.stdout)]


def test_bounded_output_keeps_the_head_and_the_tail():
    output = BoundedOutput(10)
    for chunk in (b"01234", b"56789abc", b"def"):
        output.append(chunk)
    value = output.getvalue()
    assert value.startswith(b"01234") and value.endswith(b"bcdef")
    assert b"6 bytes omitted" in value
    small = BoundedOutput(10)
    small.append(b"short")
    assert small.getvalue() == b"short"


async def test_cancelling_at_any_moment_never_hangs(tmp_path):
    # asyncio on Python 3.11 hangs when process creation is cancelled midway and
    # what it started keeps the output pipes open (as commands behind nsenter do).
    command = ["/bin/sh", "-c", f"sleep 60 & echo $$ $! >> {tmp_path}/pids; wait"]

    async def cancelled_after(steps: int) -> None:
        task = asyncio.ensure_future(run_process(command, timeout_s=30, stdin=b"x"))
        for _ in range(steps):  # into the moment the pipes are being connected
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async with asyncio.timeout(60):
        for steps in range(8):
            await asyncio.gather(*(cancelled_after(steps) for _ in range(20)))
    pids = (tmp_path / "pids").read_text().split() if (tmp_path / "pids").exists() else []
    assert [pid for pid in map(int, pids) if not await gone(pid)] == []
