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


def test_bucket_of_entry_uses_utc_hours():
    """The partition comes from the id's millisecond prefix in UTC."""
    assert rec.bucket_of_entry("1788652800000-0") == "2026-09-06T00"
    assert rec.bucket_of_entry("1788656399999-3") == "2026-09-06T00"
    assert rec.bucket_of_entry("1788656400000-0") == "2026-09-06T01"
    assert rec.bucket_of_entry("1788739200000-0") == "2026-09-07T00"


def test_bucket_names_sort_chronologically():
    """Lexical order over bucket names matches time order across days."""
    ids = ["1788652800000-0", "1788735600000-0", "1788739200000-0"]
    buckets = [rec.bucket_of_entry(i) for i in ids]
    assert buckets == sorted(buckets)
    assert buckets == ["2026-09-06T00", "2026-09-06T23", "2026-09-07T00"]


def test_bucket_end_is_one_hour_after_start():
    """A bucket stops accepting entries an hour after it starts."""
    assert rec.bucket_end("2026-09-06T00") == 1788652800.0 + 3600
    assert rec.bucket_end("2026-09-06T23") == 1788739200.0


def test_format_line_embeds_raw_payload():
    """The payload is spliced in verbatim and the line parses as JSON."""
    line = rec.format_line("1-0", "book", b'{"a":1,"b":[1.5,2]}')
    assert line.endswith(b"\n")
    assert json.loads(line) == {
        "id": "1-0",
        "type": "book",
        "data": {"a": 1, "b": [1.5, 2]},
    }


def test_last_recorded_id(tmp_path: Path):
    """The cursor resumes from the last line of the newest bucket file."""
    directory = tmp_path / "s"
    assert rec.last_recorded_id(directory) == rec.STREAM_START
    directory.mkdir()
    assert rec.last_recorded_id(directory) == rec.STREAM_START
    (directory / "2026-09-06T12.jsonl").write_bytes(
        rec.format_line("1-0", "book", b"{}") + rec.format_line("2-0", "book", b"{}")
    )
    (directory / "2026-09-06T13.jsonl").write_bytes(
        rec.format_line("3-5", "book", b"{}")
    )
    (directory / "notes.txt").write_text("ignored")
    assert rec.last_recorded_id(directory) == "3-5"
    # An empty newest file falls back to the previous one.
    (directory / "2026-09-06T14.jsonl").write_bytes(b"")
    assert rec.last_recorded_id(directory) == "3-5"


def test_last_recorded_id_reads_compressed_recordings(tmp_path: Path):
    """A compacted directory resumes from its data, not from the stream start."""
    from compression import zstd

    directory = tmp_path / "s"
    directory.mkdir()
    lines = rec.format_line("1-0", "book", b"{}")
    lines += rec.format_line("7-2", "book", b"{}")
    (directory / "2026-09-06T12.jsonl.zst").write_bytes(zstd.compress(lines))
    # Path.suffix reports ".zst" here, so a suffix match would miss the file
    # entirely and re-record everything Redis still holds.
    assert rec.last_recorded_id(directory) == "7-2"

    # An uncompressed newer bucket still wins.
    (directory / "2026-09-06T13.jsonl").write_bytes(
        rec.format_line("9-0", "book", b"{}")
    )
    assert rec.last_recorded_id(directory) == "9-0"


def test_last_line_scans_backwards_across_chunks(tmp_path: Path):
    """Long lines that span the read chunk are still returned whole."""
    path = tmp_path / "f.jsonl"
    long_line = b"x" * 10_000
    path.write_bytes(b"first\n" + long_line + b"\n")
    assert rec._last_line(path, chunk=100) == long_line


def test_stream_writer_rotates_by_hour(tmp_path: Path):
    """Entries in different UTC hours go to different files."""
    writer = rec.StreamWriter(tmp_path / "s")
    writer.write("1788652800000-0", "book", b"{}")
    writer.write("1788652800001-0", "book", b"{}")
    writer.write("1788656400000-0", "book", b"{}")
    writer.close()
    files = sorted(p.name for p in (tmp_path / "s").iterdir())
    assert files == ["2026-09-06T00.jsonl", "2026-09-06T01.jsonl"]
    assert len((tmp_path / "s" / "2026-09-06T00.jsonl").read_bytes().splitlines()) == 2


def test_seal_if_elapsed_respects_the_grace_period(tmp_path: Path):
    """A file is sealed only once its bucket ended more than the grace ago."""
    writer = rec.StreamWriter(tmp_path / "s")
    writer.write("1788652800000-0", "book", b"{}")
    end = rec.bucket_end("2026-09-06T00")
    assert writer.seal_if_elapsed(end, grace_s=300.0) is False
    assert writer.seal_if_elapsed(end + 299, grace_s=300.0) is False
    assert writer.seal_if_elapsed(end + 301, grace_s=300.0) is True
    # Idempotent: nothing is open to seal a second time.
    assert writer.seal_if_elapsed(end + 999, grace_s=300.0) is False


def test_write_after_seal_appends_rather_than_truncating(tmp_path: Path):
    """A lagging recorder crossing a sealed bucket reopens it for append."""
    directory = tmp_path / "s"
    writer = rec.StreamWriter(directory)
    writer.write("1788652800000-0", "book", b'{"n":1}')
    assert writer.seal_if_elapsed(rec.bucket_end("2026-09-06T00") + 600, 300.0) is True
    writer.write("1788652800001-0", "book", b'{"n":2}')
    writer.close()
    lines = (directory / "2026-09-06T00.jsonl").read_bytes().splitlines()
    assert [json.loads(line)["data"]["n"] for line in lines] == [1, 2]


def test_recorder_seals_every_elapsed_writer(tmp_path: Path):
    """seal_elapsed reports how many streams it closed."""
    settings = RecorderConfig(root=str(tmp_path), seal_grace_s=60.0)
    recorder = rec.Recorder(tmp_path, [STREAM, "oms:events"], settings)
    recorder.record(STREAM, "1788652800000-0", {b"type": b"book", b"data": b"{}"})
    assert recorder.seal_elapsed(rec.bucket_end("2026-09-06T00") + 30) == 0
    assert recorder.seal_elapsed(rec.bucket_end("2026-09-06T00") + 90) == 1
    assert recorder.seal_elapsed(rec.bucket_end("2026-09-06T00") + 90) == 0


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
    lines = [
        json.loads(line) for line in next(directory.iterdir()).read_bytes().splitlines()
    ]
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
    lines = [
        json.loads(line) for line in next(directory.iterdir()).read_bytes().splitlines()
    ]
    assert [line["data"]["seq"] for line in lines] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_recorder_run_loop_writes_and_flushes(tmp_path: Path):
    """The blocking loop picks up entries published while it runs."""
    redis = fakeredis.FakeRedis(decode_responses=False)
    publisher = StreamPublisher(maxlen=1000)
    settings = RecorderConfig(
        root=str(tmp_path), block_ms=20, batch=100, flush_interval_s=0.01
    )
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
