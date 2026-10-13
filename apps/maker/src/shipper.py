"""Shipper, trading host side: hand sealed recordings to the archive host.

The archive host drives every transfer. It connects over SSH and runs one of
four commands here, each a single round trip:

``pack``
    Compress every sealed ``.jsonl`` recording to ``.jsonl.zst`` at a cheap
    level, so the wire and the local disk carry roughly 25x less.
``manifest``
    Pack, then print one JSON line per sealed compressed recording with its
    path relative to the root, its size and its SHA-256.
``send``
    Read relative paths on stdin, one per line, and write those recordings
    to stdout as an uncompressed tar stream.
``prune``
    Read ``{"path", "sha256"}`` JSON lines on stdin and delete each file
    whose checksum still matches. Print one result line per request.

Only files that ``streams.sealed_files`` reports may be read or deleted, so
the recorder's live file is never touched. ``serve`` is the entry point for
a forced command in ``authorized_keys``: it runs only the verb named last in
``SSH_ORIGINAL_COMMAND``, so the archive host's key cannot open a shell on a
host that holds venue keys. See ``docs/design/event-driven-framework.md``
section 7.
"""

import argparse
import fcntl
import hashlib
import logging
import os
import shlex
import sys
import tarfile
from collections.abc import Iterable, Iterator
from compression import zstd
from pathlib import Path
from typing import IO

import msgspec

import apps.shared.src.logging_config as logging_config
from apps.shared.src.streams import (
    COMPRESSED_SUFFIX,
    FILE_SUFFIX,
    recording_directories,
    sealed_files,
)

logging_config.setup_logging()
logger = logging.getLogger(__name__)

# zstd level on the trading host. Level 3 already gives about 25x on book
# data and costs almost no CPU; the archive host recompresses at a high level.
PACK_LEVEL = 3
# Suffix of a file being written. Not a recording name, so every helper in
# streams.py ignores it.
PARTIAL_SUFFIX = ".partial"
# Held for the whole of a command, so two archive runs that overlap cannot
# pack or prune the same file at once.
LOCK_NAME = ".shipper.lock"
CHUNK = 1 << 20
VERBS = ("pack", "manifest", "send", "prune")


class ShipperError(Exception):
    """A request names a file the shipper must not read or delete."""


class ShippedFile(msgspec.Struct, frozen=True):
    """
    One sealed recording as the manifest reports it.

    Attributes
    ----------
    path : str
        Path relative to the recording root, with forward slashes.
    size : int
        Size in bytes.
    sha256 : str
        Hex SHA-256 of the file.
    """

    path: str
    size: int
    sha256: str


class PruneRequest(msgspec.Struct, frozen=True):
    """
    A request to delete one recording the archive host holds.

    Attributes
    ----------
    path : str
        Path relative to the recording root.
    sha256 : str
        Checksum the archive host verified. The file is deleted only if it
        still has this checksum.
    """

    path: str
    sha256: str


class PruneResult(msgspec.Struct, frozen=True):
    """
    The outcome of one prune request.

    Attributes
    ----------
    path : str
        Path relative to the recording root.
    status : str
        ``pruned``, ``missing``, ``changed`` or ``refused``.
    """

    path: str
    status: str


def sha256_of(path: Path) -> str:
    """
    Return the hex SHA-256 of a file.

    Parameters
    ----------
    path : Path
        File to hash.

    Returns
    -------
    str
        Hex digest.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def pack_file(path: Path, level: int = PACK_LEVEL) -> Path:
    """
    Compress one sealed recording and remove the uncompressed file.

    The compressed file is written under a partial name, synced and renamed,
    so a crash leaves either the old file alone or both files complete. The
    uncompressed file is the source in both cases, so a second run
    compresses it again and replaces the compressed one.

    Parameters
    ----------
    path : Path
        A sealed ``.jsonl`` recording.
    level : int
        zstd compression level.

    Returns
    -------
    Path
        The ``.jsonl.zst`` file.
    """
    target = path.with_name(path.name[: -len(FILE_SUFFIX)] + COMPRESSED_SUFFIX)
    partial = target.with_name(target.name + PARTIAL_SUFFIX)
    with open(path, "rb") as src, zstd.open(partial, "wb", level=level) as dst:
        while chunk := src.read(CHUNK):
            dst.write(chunk)
    with open(partial, "rb") as f:
        os.fsync(f.fileno())
    os.replace(partial, target)
    path.unlink()
    return target


def pack(root: Path, level: int = PACK_LEVEL) -> int:
    """
    Compress every sealed uncompressed recording under a root.

    Parameters
    ----------
    root : Path
        Recording root directory.
    level : int
        zstd compression level.

    Returns
    -------
    int
        Number of files compressed.
    """
    packed = 0
    for directory in recording_directories(root):
        for path in sealed_files(directory):
            if path.name.endswith(FILE_SUFFIX):
                pack_file(path, level)
                packed += 1
    return packed


def relative_name(root: Path, path: Path) -> str:
    """
    Return a recording's path relative to the root, with forward slashes.

    Parameters
    ----------
    root : Path
        Recording root directory.
    path : Path
        A recording under the root.

    Returns
    -------
    str
        The relative path.
    """
    return path.relative_to(root).as_posix()


def manifest(root: Path) -> list[ShippedFile]:
    """
    List every sealed compressed recording under a root.

    Parameters
    ----------
    root : Path
        Recording root directory.

    Returns
    -------
    list[ShippedFile]
        One entry per file, ordered by directory then bucket.
    """
    entries = []
    for directory in recording_directories(root):
        for path in sealed_files(directory):
            if path.name.endswith(COMPRESSED_SUFFIX):
                entries.append(
                    ShippedFile(
                        path=relative_name(root, path),
                        size=path.stat().st_size,
                        sha256=sha256_of(path),
                    )
                )
    return entries


def resolve_sealed(root: Path, name: str) -> Path:
    """
    Map a requested relative path to a sealed compressed recording.

    This is the only gate between a request from the archive host and the
    file system, so it refuses anything it cannot prove is safe.

    Parameters
    ----------
    root : Path
        Recording root directory.
    name : str
        Path relative to the root, as the manifest printed it.

    Returns
    -------
    Path
        The recording.

    Raises
    ------
    ShipperError
        If the path leaves the root, is not a compressed recording, or is
        not sealed.
    """
    relative = Path(name)
    # Only the exact form the manifest prints: Path would quietly collapse
    # "a//b" and "a/./b" into a name that does exist.
    canonical = relative.as_posix() == name
    if not canonical or relative.is_absolute() or ".." in relative.parts:
        raise ShipperError(f"{name!r} is not a relative path inside the root")
    if not name.endswith(COMPRESSED_SUFFIX):
        raise ShipperError(f"{name!r} is not a compressed recording")
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise ShipperError(f"{name!r} resolves outside the root")
    if path.is_symlink() or path not in sealed_files(path.parent):
        raise ShipperError(f"{name!r} is not a sealed recording")
    return path


def send(root: Path, names: Iterable[str], out: IO[bytes]) -> int:
    """
    Write the requested recordings to a stream as an uncompressed tar.

    Every name is checked before any byte is written, so a bad request
    fails without a partial stream.

    Parameters
    ----------
    root : Path
        Recording root directory.
    names : Iterable[str]
        Relative paths, as the manifest printed them.
    out : IO[bytes]
        Destination, usually stdout.

    Returns
    -------
    int
        Number of files written.
    """
    paths = [(name, resolve_sealed(root, name)) for name in names]
    with tarfile.open(fileobj=out, mode="w|") as tar:
        for name, path in paths:
            tar.add(path, arcname=name, recursive=False)
    return len(paths)


def prune(root: Path, requests: Iterable[PruneRequest]) -> list[PruneResult]:
    """
    Delete the recordings the archive host confirmed by checksum.

    Parameters
    ----------
    root : Path
        Recording root directory.
    requests : Iterable[PruneRequest]
        Files to delete and the checksum each must still have.

    Returns
    -------
    list[PruneResult]
        One result per request, in request order.
    """
    results = []
    for request in requests:
        try:
            path = resolve_sealed(root, request.path)
        except ShipperError as error:
            status = "missing" if not (root / request.path).exists() else "refused"
            if status == "refused":
                logger.warning(f"Refused to prune: {error}")
            results.append(PruneResult(path=request.path, status=status))
            continue
        if sha256_of(path) != request.sha256:
            logger.warning(f"Not pruning {request.path}: checksum changed")
            results.append(PruneResult(path=request.path, status="changed"))
            continue
        path.unlink()
        results.append(PruneResult(path=request.path, status="pruned"))
    return results


def _lines(stream: IO[bytes]) -> Iterator[bytes]:
    """
    Yield the non-empty lines of a stream without their newline.

    Parameters
    ----------
    stream : IO[bytes]
        Input, usually stdin.

    Yields
    ------
    bytes
        One line.
    """
    for line in stream:
        stripped = line.strip()
        if stripped:
            yield stripped


def run(verb: str, root: Path, stdin: IO[bytes], stdout: IO[bytes]) -> None:
    """
    Run one shipper command.

    Parameters
    ----------
    verb : str
        One of ``VERBS``.
    root : Path
        Recording root directory.
    stdin : IO[bytes]
        Request input.
    stdout : IO[bytes]
        Reply output.

    Raises
    ------
    ShipperError
        If the verb is unknown or a request names a file it must not touch.
    """
    if verb not in VERBS:
        raise ShipperError(f"Unknown command {verb!r}, expected one of {VERBS}")
    root.mkdir(parents=True, exist_ok=True)
    with open(root / LOCK_NAME, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        _run_locked(verb, root, stdin, stdout)


def _run_locked(verb: str, root: Path, stdin: IO[bytes], stdout: IO[bytes]) -> None:
    """
    Run one shipper command while holding the root's lock.

    Parameters
    ----------
    verb : str
        One of ``VERBS``.
    root : Path
        Recording root directory.
    stdin : IO[bytes]
        Request input.
    stdout : IO[bytes]
        Reply output.
    """
    encoder = msgspec.json.Encoder()
    match verb:
        case "pack":
            logger.info(f"Packed {pack(root)} recordings")
        case "manifest":
            pack(root)
            for entry in manifest(root):
                stdout.write(encoder.encode(entry) + b"\n")
        case "send":
            names = [line.decode() for line in _lines(stdin)]
            send(root, names, stdout)
        case "prune":
            decoder = msgspec.json.Decoder(PruneRequest)
            requests = [decoder.decode(line) for line in _lines(stdin)]
            for result in prune(root, requests):
                stdout.write(encoder.encode(result) + b"\n")
    stdout.flush()


def verb_of_ssh_command(command: str | None) -> str:
    """
    Pick the verb out of the command an SSH client asked for.

    The archive host sends its configured remote command followed by the
    verb. Under a forced command only the last word is honoured, so the
    client cannot run anything else.

    Parameters
    ----------
    command : str | None
        Value of ``SSH_ORIGINAL_COMMAND``.

    Returns
    -------
    str
        The verb.

    Raises
    ------
    ShipperError
        If no command was given.
    """
    words = shlex.split(command or "")
    if not words:
        raise ShipperError("No command given; expected one of " + ", ".join(VERBS))
    return words[-1]


def main(argv: list[str] | None = None) -> int:
    """
    Run the shipper from the command line.

    Parameters
    ----------
    argv : list[str] | None
        Arguments, ``sys.argv[1:]`` by default.

    Returns
    -------
    int
        Process exit status.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("verb", choices=(*VERBS, "serve"))
    parser.add_argument(
        "--root",
        type=Path,
        help="Recording root. Defaults to recorder.root from config.toml.",
    )
    args = parser.parse_args(argv)
    if args.root is None:
        from apps.shared.src.config import load_app_config

        args.root = Path(load_app_config().recorder.root)
    verb = args.verb
    try:
        if verb == "serve":
            verb = verb_of_ssh_command(os.environ.get("SSH_ORIGINAL_COMMAND"))
        run(verb, args.root, sys.stdin.buffer, sys.stdout.buffer)
    except ShipperError as error:
        logger.error(str(error))
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
