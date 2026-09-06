"""Stream recorder: the durable copy of everything on the bus.

One process ``XREAD``s every configured stream and appends each entry to a
JSON Lines file partitioned by stream and UTC day, for example
``data/md/book/mexc/ALPH-USDT/2026-09-06.jsonl``. Entries are written
verbatim, without decoding the payload, so the recorder stays cheap and never
rejects an event a newer producer emits.

Each line is ``{"id": "<redis stream id>", "type": "<tag>", "data": <json>}``.
On startup the recorder resumes each stream from the last id it recorded, so
a restart neither duplicates nor drops entries that Redis still holds.
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from redis.asyncio import ConnectionPool, Redis

import apps.shared.src.logging_config as logging_config
from apps.shared.src.config import AppConfig, RecorderConfig, load_app_config
from apps.shared.src.events import DATA_FIELD, TYPE_FIELD
from apps.shared.src.streams import configured_streams, stream_path
from apps.shared.src.utils import production

logging_config.setup_logging()
logger = logging.getLogger(__name__)

FILE_SUFFIX = ".jsonl"
# Start of a stream, for a stream that has never been recorded.
STREAM_START = "0-0"


def day_of_entry(entry_id: str) -> str:
    """
    Return the UTC day a stream entry was added on.

    Parameters
    ----------
    entry_id : str
        Redis stream id, ``<ms>-<seq>``.

    Returns
    -------
    str
        ``YYYY-MM-DD``.
    """
    ms = int(entry_id.split("-", 1)[0])
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def format_line(entry_id: str, event_type: str, data: bytes) -> bytes:
    """
    Build one JSON Lines record without re-encoding the payload.

    Parameters
    ----------
    entry_id : str
        Redis stream id.
    event_type : str
        Value of the entry's ``type`` field.
    data : bytes
        Raw JSON payload from the entry's ``data`` field.

    Returns
    -------
    bytes
        The line, newline terminated.
    """
    head = json.dumps({"id": entry_id, "type": event_type}, separators=(",", ":"))
    return b"".join((head[:-1].encode(), b',"data":', data, b"}\n"))


def last_recorded_id(directory: Path) -> str:
    """
    Find the id of the last entry recorded in a stream directory.

    Reads the tail of the newest day file. A corrupt or empty tail is
    treated as no recording, which at worst duplicates the entries Redis
    still holds.

    Parameters
    ----------
    directory : Path
        Directory returned by ``stream_path``.

    Returns
    -------
    str
        The last id, or ``STREAM_START`` if nothing was recorded.
    """
    if not directory.is_dir():
        return STREAM_START
    files = sorted(p for p in directory.iterdir() if p.suffix == FILE_SUFFIX)
    for path in reversed(files):
        tail = _last_line(path)
        if tail is None:
            continue
        try:
            entry_id = json.loads(tail)["id"]
        except (ValueError, KeyError, TypeError):
            logger.warning(f"Unreadable last line in {path}, resuming from start")
            return STREAM_START
        return str(entry_id)
    return STREAM_START


def _last_line(path: Path, chunk: int = 4096) -> bytes | None:
    """
    Return the last non-empty line of a file, or None if it has none.

    Parameters
    ----------
    path : Path
        File to read.
    chunk : int
        Bytes read per step while scanning backwards.

    Returns
    -------
    bytes | None
        The line without its newline.
    """
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return None
        buffer = b""
        position = size
        while position > 0:
            step = min(chunk, position)
            position -= step
            f.seek(position)
            buffer = f.read(step) + buffer
            lines = buffer.rstrip(b"\n").split(b"\n")
            if len(lines) > 1 or position == 0:
                last = lines[-1]
                return last if last else None
    return None


class StreamWriter:
    """
    Append-only writer for one stream, rotating files by UTC day.

    Attributes
    ----------
    directory : Path
        Directory the day files live in.
    """

    def __init__(self, directory: Path) -> None:
        """
        Initialize the writer.

        Parameters
        ----------
        directory : Path
            Directory returned by ``stream_path``. Created on first write.
        """
        self.directory = directory
        self._day: str | None = None
        self._file: Any = None

    def write(self, entry_id: str, event_type: str, data: bytes) -> None:
        """
        Append one entry, opening a new day file when the day changes.

        Parameters
        ----------
        entry_id : str
            Redis stream id.
        event_type : str
            Value of the entry's ``type`` field.
        data : bytes
            Raw JSON payload.
        """
        day = day_of_entry(entry_id)
        if day != self._day:
            self.close()
            self.directory.mkdir(parents=True, exist_ok=True)
            self._file = open(self.directory / f"{day}{FILE_SUFFIX}", "ab")
            self._day = day
        self._file.write(format_line(entry_id, event_type, data))

    def flush(self) -> None:
        """Flush buffered lines to the operating system."""
        if self._file is not None:
            self._file.flush()

    def close(self) -> None:
        """Flush and close the current file, if any."""
        if self._file is not None:
            self._file.close()
            self._file = None
            self._day = None


class Recorder:
    """
    Read a set of streams and persist every entry.

    Attributes
    ----------
    root : Path
        Recording root directory.
    cursors : dict[str, str]
        Last consumed id per stream, the ``XREAD`` start positions.
    """

    def __init__(self, root: Path, streams: list[str], settings: RecorderConfig) -> None:
        """
        Initialize the recorder and resume positions from disk.

        Parameters
        ----------
        root : Path
            Recording root directory.
        streams : list[str]
            Unprefixed stream names to record.
        settings : RecorderConfig
            Read and flush tuning.
        """
        self.root = root
        self.settings = settings
        self.writers: dict[str, StreamWriter] = {}
        self.cursors: dict[str, str] = {}
        for stream in streams:
            directory = stream_path(root, stream)
            self.writers[stream] = StreamWriter(directory)
            self.cursors[stream] = last_recorded_id(directory)
        self.entries_written = 0

    def record(self, stream: str, entry_id: str, fields: dict[bytes, bytes]) -> None:
        """
        Persist one entry and advance the stream cursor.

        Parameters
        ----------
        stream : str
            Unprefixed stream name.
        entry_id : str
            Redis stream id.
        fields : dict[bytes, bytes]
            Raw field map from ``XREAD``.
        """
        event_type = fields.get(TYPE_FIELD.encode(), b"").decode()
        data = fields.get(DATA_FIELD.encode(), b"null")
        self.writers[stream].write(entry_id, event_type, data)
        self.cursors[stream] = entry_id
        self.entries_written += 1

    def record_batch(self, response: list[Any]) -> int:
        """
        Persist the result of one ``XREAD``.

        Parameters
        ----------
        response : list[Any]
            ``XREAD`` reply: ``[[stream, [(id, fields), ...]], ...]`` with
            bytes for stream names and ids.

        Returns
        -------
        int
            Number of entries written.
        """
        written = 0
        for stream_raw, entries in response:
            stream = stream_raw.decode() if isinstance(stream_raw, bytes) else stream_raw
            for entry_id_raw, fields in entries:
                entry_id = (
                    entry_id_raw.decode()
                    if isinstance(entry_id_raw, bytes)
                    else entry_id_raw
                )
                self.record(stream, entry_id, fields)
                written += 1
        return written

    def flush(self) -> None:
        """Flush every open file."""
        for writer in self.writers.values():
            writer.flush()

    def close(self) -> None:
        """Flush and close every open file."""
        for writer in self.writers.values():
            writer.close()

    async def run(self, redis: Redis) -> None:
        """
        Read and record until cancelled.

        Parameters
        ----------
        redis : Redis
            A Redis client created with ``decode_responses=False``.
        """
        flusher = asyncio.create_task(self._flush_periodically())
        try:
            while True:
                response = await redis.xread(
                    cast(dict[Any, Any], dict(self.cursors)),
                    count=self.settings.batch,
                    block=self.settings.block_ms,
                )
                if response:
                    # redis-py types XREAD as list or dict (RESP3); the client
                    # is RESP2 here so it is always the list form.
                    self.record_batch(cast(list[Any], response))
        finally:
            flusher.cancel()
            self.close()

    async def _flush_periodically(self) -> None:
        """Flush open files on the configured interval."""
        while True:
            await asyncio.sleep(self.settings.flush_interval_s)
            self.flush()


async def main(config: AppConfig) -> None:
    """
    Run the recorder against the configured Redis.

    Parameters
    ----------
    config : AppConfig
        The application configuration.
    """
    streams = configured_streams(config, production)
    root = Path(config.recorder.root)
    recorder = Recorder(root, streams, config.recorder)
    logger.info(f"Recording {len(streams)} streams under {root.resolve()}")
    for stream, cursor in recorder.cursors.items():
        logger.info(f"  {stream} from {cursor}")

    pool = ConnectionPool(
        host=config.redis.host, port=config.redis.port, db=0, max_connections=4
    )
    redis = Redis(decode_responses=False, connection_pool=pool)

    # The orchestrator stops processes with SIGTERM. Turn it into a task
    # cancellation so ``Recorder.run`` reaches its ``finally`` and flushes the
    # last buffered lines instead of dying with them in memory.
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: task.cancel() if task else None)
    try:
        await recorder.run(redis)
    except asyncio.CancelledError:
        logger.info(f"Recorder stopped after {recorder.entries_written} entries")
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main(load_app_config()))
