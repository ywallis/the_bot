"""Tests for the stream recorder."""

import asyncio
import json
from pathlib import Path

import pytest
from fakeredis import aioredis as fakeredis

from apps.maker.src import recorder as rec
from apps.shared.src.config import RecorderConfig
from apps.shared.src.events import BookEvent
from apps.shared.src.streams import StreamPublisher, stream_path

TS = 1_757_160_000_000_000_000
STREAM = "md:book:mexc:ALPH/USDT"


def book(seq: int) -> BookEvent:
    """Return a small book event."""
    return BookEvent(
        ts_recv=TS + seq,
        venue="mexc",
        symbol="ALPH/USDT",
        seq=seq,
        ts_exch=None,
        bids=[(1.0, 1.0)],
        asks=[(1.1, 1.0)],
    )


def test_day_of_entry_uses_utc():
    """The day partition comes from the id's millisecond prefix in UTC."""
    assert rec.day_of_entry("1788652800000-0") == "2026-09-06"
    assert rec.day_of_entry("1788739199999-3") == "2026-09-06"
    assert rec.day_of_entry("1788739200000-0") == "2026-09-07"


def test_format_line_embeds_raw_payload():
    """The payload is spliced in verbatim and the line parses as JSON."""
    line = rec.format_line("1-0", "book", b'{"a":1,"b":[1.5,2]}')
    assert line.endswith(b"\n")
    assert json.loads(line) == {"id": "1-0", "type": "book", "data": {"a": 1, "b": [1.5, 2]}}


def test_last_recorded_id(tmp_path: Path):
    """The cursor resumes from the last line of the newest day file."""
    directory = tmp_path / "s"
    assert rec.last_recorded_id(directory) == rec.STREAM_START
    directory.mkdir()
    assert rec.last_recorded_id(directory) == rec.STREAM_START
    (directory / "2026-09-05.jsonl").write_bytes(
        rec.format_line("1-0", "book", b"{}") + rec.format_line("2-0", "book", b"{}")
    )
    (directory / "2026-09-06.jsonl").write_bytes(rec.format_line("3-5", "book", b"{}"))
    (directory / "notes.txt").write_text("ignored")
    assert rec.last_recorded_id(directory) == "3-5"
    # An empty newest file falls back to the previous one.
    (directory / "2026-09-07.jsonl").write_bytes(b"")
    assert rec.last_recorded_id(directory) == "3-5"


def test_last_line_scans_backwards_across_chunks(tmp_path: Path):
    """Long lines that span the read chunk are still returned whole."""
    path = tmp_path / "f.jsonl"
    long_line = b"x" * 10_000
    path.write_bytes(b"first\n" + long_line + b"\n")
    assert rec._last_line(path, chunk=100) == long_line


def test_stream_writer_rotates_by_day(tmp_path: Path):
    """Entries on different UTC days go to different files."""
    writer = rec.StreamWriter(tmp_path / "s")
    writer.write("1788652800000-0", "book", b"{}")
    writer.write("1788652800001-0", "book", b"{}")
    writer.write("1788739200000-0", "book", b"{}")
    writer.close()
    files = sorted(p.name for p in (tmp_path / "s").iterdir())
    assert files == ["2026-09-06.jsonl", "2026-09-07.jsonl"]
    assert len((tmp_path / "s" / "2026-09-06.jsonl").read_bytes().splitlines()) == 2


@pytest.mark.asyncio
async def test_recorder_records_batches_and_resumes(tmp_path: Path):
    """Entries are written from the cursor onward and a restart resumes."""
    redis = fakeredis.FakeRedis(decode_responses=False)
    publisher = StreamPublisher(maxlen=1000)
    for seq in range(1, 4):
        await publisher.publish(redis, book(seq))

    settings = RecorderConfig(root=str(tmp_path), block_ms=10, batch=2)
    recorder = rec.Recorder(tmp_path, [STREAM, "oms:events"], settings)
    assert recorder.cursors[STREAM] == rec.STREAM_START

    response = await redis.xread(dict(recorder.cursors), count=settings.batch)
    assert recorder.record_batch(response) == 2
    response = await redis.xread(dict(recorder.cursors), count=settings.batch)
    assert recorder.record_batch(response) == 1
    recorder.close()

    directory = stream_path(tmp_path, STREAM)
    lines = [json.loads(line) for line in next(directory.iterdir()).read_bytes().splitlines()]
    assert [line["data"]["seq"] for line in lines] == [1, 2, 3]
    assert all(line["type"] == "book" for line in lines)
    last_id = lines[-1]["id"]

    # A second recorder over the same directory starts after the last id.
    await publisher.publish(redis, book(4))
    restarted = rec.Recorder(tmp_path, [STREAM], settings)
    assert restarted.cursors[STREAM] == last_id
    response = await redis.xread(dict(restarted.cursors), count=10)
    assert restarted.record_batch(response) == 1
    restarted.close()
    lines = [json.loads(line) for line in next(directory.iterdir()).read_bytes().splitlines()]
    assert [line["data"]["seq"] for line in lines] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_recorder_run_loop_writes_and_flushes(tmp_path: Path):
    """The blocking loop picks up entries published while it runs."""
    redis = fakeredis.FakeRedis(decode_responses=False)
    publisher = StreamPublisher(maxlen=1000)
    settings = RecorderConfig(root=str(tmp_path), block_ms=20, batch=100, flush_interval_s=0.01)
    recorder = rec.Recorder(tmp_path, [STREAM], settings)

    task = asyncio.create_task(recorder.run(redis))
    await asyncio.sleep(0.05)
    await publisher.publish(redis, book(1))
    await publisher.publish(redis, book(2))
    for _ in range(50):
        await asyncio.sleep(0.02)
        if recorder.entries_written == 2:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    directory = stream_path(tmp_path, STREAM)
    lines = next(directory.iterdir()).read_bytes().splitlines()
    assert len(lines) == 2
