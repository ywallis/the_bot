"""Archive host: pull sealed recordings from trading hosts and keep them.

For each configured source the archive host runs the shipper three times
(``apps/maker/src/shipper.py``), over SSH or as a local process:

1. ``manifest`` packs the source's sealed recordings and lists them with
   their checksums.
2. ``send`` streams the files the archive does not hold yet. Each is checked
   against the manifest checksum, recompressed at ``archive.level`` and
   recorded in the ledger before the next one is read.
3. ``prune`` deletes from the source exactly the files the ledger holds with
   the checksum the source still reports.

A file therefore leaves a trading host only after the archive holds it and
has verified it, and an interrupted run loses nothing: the next run finds the
same files in the manifest and continues. The layout under the archive root
is ``<source>/<stream path>/<bucket>.jsonl.zst``, so every recording tool
reads an archived source with ``--root <archive root>/<source>``.

Run it from cron or a systemd timer on the archive host::

    uv run -m apps.accountant.src.archive [--config archive.toml]

The archive host needs no venues or strategies, so ``--config`` can name a
file that holds only the ``[archive]`` table.
"""

import argparse
import fcntl
import hashlib
import logging
import os
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from compression import zstd
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

import msgspec

import apps.shared.src.logging_config as logging_config
from apps.maker.src.shipper import (
    CHUNK,
    PACK_LEVEL,
    PARTIAL_SUFFIX,
    PruneRequest,
    PruneResult,
    ShippedFile,
)
from apps.shared.src.config import ArchiveConfig, ArchiveSource, load_archive_config
from apps.shared.src.streams import COMPRESSED_SUFFIX

logging_config.setup_logging()
logger = logging.getLogger(__name__)

LEDGER_NAME = "ledger.sqlite"
LOCK_NAME = ".archive.lock"
STAGING_NAME = ".staging"


class ArchiveError(Exception):
    """A source could not be pulled."""


class Ledger:
    """
    The archive's record of every file it holds and where it came from.

    Attributes
    ----------
    connection : sqlite3.Connection
        Open connection to the ledger database.
    """

    def __init__(self, path: Path) -> None:
        """
        Open the ledger, creating it if needed.

        Parameters
        ----------
        path : Path
            Database file.
        """
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS files ("
            " source TEXT NOT NULL,"
            " path TEXT NOT NULL,"
            " sha256 TEXT NOT NULL,"
            " size INTEGER NOT NULL,"
            " archived_at TEXT NOT NULL,"
            " pruned_at TEXT,"
            " PRIMARY KEY (source, path))"
        )
        self.connection.commit()

    def checksum(self, source: str, path: str) -> str | None:
        """
        Return the source checksum of an archived file.

        Parameters
        ----------
        source : str
            Source name.
        path : str
            Path relative to the source's recording root.

        Returns
        -------
        str | None
            The checksum the file had on the source, or None if it is not
            archived.
        """
        row = self.connection.execute(
            "SELECT sha256 FROM files WHERE source = ? AND path = ?", (source, path)
        ).fetchone()
        return row[0] if row else None

    def record(self, source: str, entry: ShippedFile) -> None:
        """
        Record that a file is archived, replacing an earlier version.

        Parameters
        ----------
        source : str
            Source name.
        entry : ShippedFile
            The file as the source's manifest reported it.
        """
        self.connection.execute(
            "INSERT OR REPLACE INTO files"
            " (source, path, sha256, size, archived_at, pruned_at)"
            " VALUES (?, ?, ?, ?, ?, NULL)",
            (source, entry.path, entry.sha256, entry.size, _now()),
        )
        self.connection.commit()

    def mark_pruned(self, source: str, path: str) -> None:
        """
        Record that a source deleted its copy of an archived file.

        Parameters
        ----------
        source : str
            Source name.
        path : str
            Path relative to the source's recording root.
        """
        self.connection.execute(
            "UPDATE files SET pruned_at = ? WHERE source = ? AND path = ?",
            (_now(), source, path),
        )
        self.connection.commit()

    def close(self) -> None:
        """Close the database."""
        self.connection.close()


@dataclass
class PullReport:
    """
    What one pull of one source did.

    Attributes
    ----------
    source : str
        Source name.
    listed : int
        Files in the source's manifest.
    archived : list[str]
        Files fetched, verified and stored.
    pruned : list[str]
        Files the source deleted.
    failed : list[str]
        Files that did not arrive or did not match their checksum.
    """

    source: str
    listed: int = 0
    archived: list[str] = field(default_factory=list)
    pruned: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


def _now() -> str:
    """
    Return the current UTC time in ISO format.

    Returns
    -------
    str
        The time.
    """
    return datetime.now(UTC).isoformat(timespec="seconds")


def shipper_command(source: ArchiveSource, verb: str) -> list[str]:
    """
    Build the command line that runs the shipper for a source.

    Parameters
    ----------
    source : ArchiveSource
        The source.
    verb : str
        Shipper verb.

    Returns
    -------
    list[str]
        Arguments for ``subprocess``.
    """
    if source.ssh:
        return ["ssh", "-o", "BatchMode=yes", source.ssh, f"{source.command} {verb}"]
    return [
        sys.executable,
        "-m",
        "apps.maker.src.shipper",
        "--root",
        source.root,
        verb,
    ]


def _call(source: ArchiveSource, verb: str, request: bytes = b"") -> bytes:
    """
    Run a shipper verb to completion and return its output.

    Parameters
    ----------
    source : ArchiveSource
        The source.
    verb : str
        Shipper verb.
    request : bytes
        Standard input for the shipper.

    Returns
    -------
    bytes
        Standard output.

    Raises
    ------
    ArchiveError
        If the shipper exits with an error.
    """
    done = subprocess.run(
        shipper_command(source, verb), input=request, capture_output=True
    )
    if done.returncode != 0:
        raise ArchiveError(
            f"{source.name}: shipper {verb} exited {done.returncode}: "
            f"{done.stderr.decode(errors='replace').strip()}"
        )
    return done.stdout


def is_safe_name(name: str) -> bool:
    """
    Check that a path from a source stays inside the archive.

    A source is a trading host on the internet, so its manifest is checked
    like any other untrusted input before a path from it is written to.

    Parameters
    ----------
    name : str
        Path as the manifest printed it.

    Returns
    -------
    bool
        True for a relative path of plain components naming a compressed
        recording.
    """
    parts = name.split("/")
    return (
        name.endswith(COMPRESSED_SUFFIX)
        and not name.startswith("/")
        and all(part not in ("", ".", "..") and "\\" not in part for part in parts)
    )


def fetch_manifest(source: ArchiveSource) -> list[ShippedFile]:
    """
    Ask a source to pack its sealed recordings and list them.

    Parameters
    ----------
    source : ArchiveSource
        The source.

    Returns
    -------
    list[ShippedFile]
        The source's sealed compressed recordings.

    Raises
    ------
    ArchiveError
        If the manifest names a path outside the archive.
    """
    decoder = msgspec.json.Decoder(ShippedFile)
    entries = []
    for line in _call(source, "manifest").splitlines():
        if not line.strip():
            continue
        entry = decoder.decode(line)
        if not is_safe_name(entry.path):
            raise ArchiveError(f"{source.name}: unsafe path {entry.path!r}")
        entries.append(entry)
    return entries


def store(staged: Path, target: Path, level: int) -> None:
    """
    Move a verified file into the archive, recompressing it if asked.

    Parameters
    ----------
    staged : Path
        The file as the source sent it, compressed at the source's level.
    target : Path
        Final location in the archive.
    level : int
        zstd level to store at. At or below the source's level the file is
        moved as it is.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + PARTIAL_SUFFIX)
    if level <= PACK_LEVEL:
        os.replace(staged, partial)
    else:
        with (
            zstd.open(staged, "rb") as src,
            zstd.open(partial, "wb", level=level) as dst,
        ):
            while chunk := src.read(CHUNK):
                dst.write(chunk)
        staged.unlink()
    with open(partial, "rb") as f:
        os.fsync(f.fileno())
    os.replace(partial, target)


def _receive(member_file: IO[bytes], staged: Path, expected: ShippedFile) -> bool:
    """
    Write one file from the tar stream to staging and check it.

    Parameters
    ----------
    member_file : IO[bytes]
        The member's content.
    staged : Path
        Where to write it.
    expected : ShippedFile
        The manifest entry it must match.

    Returns
    -------
    bool
        True if size and checksum match the manifest.
    """
    digest = hashlib.sha256()
    size = 0
    with open(staged, "wb") as out:
        while chunk := member_file.read(CHUNK):
            digest.update(chunk)
            size += len(chunk)
            out.write(chunk)
    return size == expected.size and digest.hexdigest() == expected.sha256


def fetch(
    source: ArchiveSource,
    wanted: dict[str, ShippedFile],
    root: Path,
    level: int,
    ledger: Ledger,
    report: PullReport,
) -> None:
    """
    Stream the wanted files from a source into the archive.

    Each file is verified and recorded in the ledger as soon as it arrives,
    so a transfer that breaks halfway keeps everything before the break.

    Parameters
    ----------
    source : ArchiveSource
        The source.
    wanted : dict[str, ShippedFile]
        Manifest entries to fetch, by path.
    root : Path
        Archive root.
    level : int
        zstd level to store at.
    ledger : Ledger
        The archive ledger.
    report : PullReport
        Updated with what was archived and what failed.

    Raises
    ------
    ArchiveError
        If the shipper exits with an error.
    """
    staging = root / STAGING_NAME / source.name
    staging.mkdir(parents=True, exist_ok=True)
    request = "".join(f"{path}\n" for path in wanted).encode()
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(
            shipper_command(source, "send"),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=errors,
        )
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write(request)
        process.stdin.close()
        received: set[str] = set()
        try:
            with tarfile.open(fileobj=process.stdout, mode="r|") as tar:
                for member in tar:
                    expected = wanted.get(member.name)
                    member_file = tar.extractfile(member) if member.isfile() else None
                    if expected is None or member_file is None:
                        logger.warning(f"{source.name}: unrequested {member.name!r}")
                        continue
                    received.add(member.name)
                    staged = staging / expected.sha256
                    if not _receive(member_file, staged, expected):
                        logger.error(f"{source.name}: {member.name} failed its check")
                        staged.unlink()
                        report.failed.append(member.name)
                        continue
                    store(staged, root / source.name / member.name, level)
                    ledger.record(source.name, expected)
                    report.archived.append(member.name)
        except tarfile.TarError as error:
            logger.error(f"{source.name}: transfer broke off: {error}")
        finally:
            process.stdout.close()
            status = process.wait()
        if status != 0:
            errors.seek(0)
            raise ArchiveError(
                f"{source.name}: shipper send exited {status}: "
                f"{errors.read().decode(errors='replace').strip()}"
            )
    report.failed.extend(sorted(set(wanted) - received - set(report.failed)))


def prune_source(
    source: ArchiveSource,
    entries: Sequence[ShippedFile],
    root: Path,
    ledger: Ledger,
    report: PullReport,
) -> None:
    """
    Delete from a source every listed file the archive holds unchanged.

    Parameters
    ----------
    source : ArchiveSource
        The source.
    entries : Sequence[ShippedFile]
        The source's manifest.
    root : Path
        Archive root.
    ledger : Ledger
        The archive ledger.
    report : PullReport
        Updated with what was pruned.
    """
    requests = [
        PruneRequest(path=entry.path, sha256=entry.sha256)
        for entry in entries
        if ledger.checksum(source.name, entry.path) == entry.sha256
        and (root / source.name / entry.path).is_file()
    ]
    if not requests:
        return
    encoder = msgspec.json.Encoder()
    reply = _call(
        source, "prune", b"".join(encoder.encode(r) + b"\n" for r in requests)
    )
    decoder = msgspec.json.Decoder(PruneResult)
    for line in reply.splitlines():
        if not line.strip():
            continue
        result = decoder.decode(line)
        if result.status in ("pruned", "missing"):
            ledger.mark_pruned(source.name, result.path)
            report.pruned.append(result.path)
        else:
            logger.warning(f"{source.name}: {result.path} not pruned: {result.status}")


def pull(
    source: ArchiveSource, settings: ArchiveConfig, ledger: Ledger, prune: bool
) -> PullReport:
    """
    Archive everything a source has sealed, then prune what was archived.

    Parameters
    ----------
    source : ArchiveSource
        The source.
    settings : ArchiveConfig
        Archive settings.
    ledger : Ledger
        The archive ledger.
    prune : bool
        Whether to delete archived files from the source.

    Returns
    -------
    PullReport
        What was done.
    """
    root = Path(settings.root)
    report = PullReport(source=source.name)
    entries = fetch_manifest(source)
    report.listed = len(entries)
    wanted = {}
    for entry in entries:
        held = ledger.checksum(source.name, entry.path)
        if held == entry.sha256:
            continue
        if held is not None:
            logger.warning(
                f"{source.name}: {entry.path} changed since it was archived; "
                "replacing the archived copy"
            )
        wanted[entry.path] = entry
    if wanted:
        fetch(source, wanted, root, settings.level, ledger, report)
    if prune:
        prune_source(source, entries, root, ledger, report)
    return report


def run(settings: ArchiveConfig, names: Sequence[str], prune: bool) -> list[PullReport]:
    """
    Pull every selected source, one after the other.

    Only one run holds the archive at a time; a second one waits.

    Parameters
    ----------
    settings : ArchiveConfig
        Archive settings.
    names : Sequence[str]
        Sources to pull. Empty means all.
    prune : bool
        Whether to delete archived files from the sources.

    Returns
    -------
    list[PullReport]
        One report per source that answered.

    Raises
    ------
    ArchiveError
        If a requested source is not configured.
    """
    unknown = set(names) - {source.name for source in settings.sources}
    if unknown:
        raise ArchiveError(f"Unknown archive sources {sorted(unknown)}")
    root = Path(settings.root)
    root.mkdir(parents=True, exist_ok=True)
    reports = []
    with open(root / LOCK_NAME, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        ledger = Ledger(root / LEDGER_NAME)
        try:
            for source in settings.sources:
                if names and source.name not in names:
                    continue
                try:
                    reports.append(pull(source, settings, ledger, prune))
                except ArchiveError as error:
                    logger.error(str(error))
        finally:
            ledger.close()
    return reports


def main(argv: list[str] | None = None) -> int:
    """
    Pull the configured sources from the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` by default.

    Returns
    -------
    int
        0 if every source was pulled cleanly, 1 otherwise.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source", action="append", default=[], help="Source to pull; repeatable."
    )
    parser.add_argument(
        "--no-prune", action="store_true", help="Keep archived files on the source."
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="TOML file with an [archive] table. Defaults to config.toml.",
    )
    args = parser.parse_args(argv)
    settings = load_archive_config(args.config)
    if not settings.sources:
        logger.error("No [[archive.sources]] in the configuration")
        return 1
    reports = run(settings, args.source, settings.prune and not args.no_prune)
    for report in reports:
        print(
            f"{report.source}: {report.listed} listed, {len(report.archived)} "
            f"archived, {len(report.pruned)} pruned, {len(report.failed)} failed"
        )
    clean = len(reports) == len(args.source or settings.sources)
    return 0 if clean and not any(report.failed for report in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
