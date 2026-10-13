"""Tests for the trading host side of the shipper."""

import io
import tarfile
from pathlib import Path

import pytest

from apps.maker.src import shipper
from apps.maker.src.shipper import PruneRequest, ShipperError
from apps.shared.src.streams import open_recording, recording_files

BOOK = "md/book/venue_a/BASE-QUOTE"
TRADE = "md/trade/venue_a/BASE-QUOTE"
BUCKETS = ("2026-09-06T00", "2026-09-06T01", "2026-09-06T02")


def lines(bucket: str) -> bytes:
    """Return the content of a small recording for a bucket."""
    return b"".join(
        f'{{"id":"{i}-0","type":"book","data":{{"b":"{bucket}"}}}}\n'.encode()
        for i in range(50)
    )


def make_recording(root: Path, stream: str = BOOK) -> Path:
    """Write one uncompressed file per bucket and return the directory."""
    directory = root / stream
    directory.mkdir(parents=True)
    for bucket in BUCKETS:
        (directory / f"{bucket}.jsonl").write_bytes(lines(bucket))
    return directory


def test_pack_compresses_sealed_files_and_leaves_the_live_one(tmp_path: Path):
    """The newest bucket stays uncompressed for the recorder to append to."""
    directory = make_recording(tmp_path)

    assert shipper.pack(tmp_path) == 2

    names = [path.name for path in recording_files(directory)]
    assert names == [
        "2026-09-06T00.jsonl.zst",
        "2026-09-06T01.jsonl.zst",
        "2026-09-06T02.jsonl",
    ]
    with open_recording(directory / "2026-09-06T00.jsonl.zst") as f:
        assert f.read() == lines("2026-09-06T00")
    assert shipper.pack(tmp_path) == 0


def test_pack_redoes_a_compression_interrupted_before_unlink(tmp_path: Path):
    """The uncompressed file is the source when both versions exist."""
    directory = make_recording(tmp_path)
    (directory / "2026-09-06T00.jsonl.zst").write_bytes(b"truncated")

    shipper.pack(tmp_path)

    assert not (directory / "2026-09-06T00.jsonl").exists()
    with open_recording(directory / "2026-09-06T00.jsonl.zst") as f:
        assert f.read() == lines("2026-09-06T00")


def test_manifest_lists_sealed_compressed_files_with_checksums(tmp_path: Path):
    """Uncompressed and live files are never listed."""
    make_recording(tmp_path, BOOK)
    make_recording(tmp_path, TRADE)
    shipper.pack(tmp_path)

    entries = shipper.manifest(tmp_path)

    assert [entry.path for entry in entries] == [
        f"{BOOK}/2026-09-06T00.jsonl.zst",
        f"{BOOK}/2026-09-06T01.jsonl.zst",
        f"{TRADE}/2026-09-06T00.jsonl.zst",
        f"{TRADE}/2026-09-06T01.jsonl.zst",
    ]
    first = tmp_path / entries[0].path
    assert entries[0].sha256 == shipper.sha256_of(first)
    assert entries[0].size == first.stat().st_size


@pytest.mark.parametrize(
    "name",
    [
        f"{BOOK}/2026-09-06T02.jsonl",
        f"{BOOK}/2026-09-06T01.jsonl",
        f"{BOOK}/../../../etc/passwd.jsonl.zst",
        f"/{BOOK}/2026-09-06T00.jsonl.zst",
        f"{BOOK}//2026-09-06T00.jsonl.zst",
        f"{BOOK}/2026-09-07T00.jsonl.zst",
    ],
)
def test_resolve_sealed_refuses_anything_unsafe(tmp_path: Path, name: str):
    """Live, uncompressed, missing and escaping paths are all refused."""
    directory = make_recording(tmp_path)
    shipper.pack_file(directory / "2026-09-06T00.jsonl")

    with pytest.raises(ShipperError):
        shipper.resolve_sealed(tmp_path, name)


def test_resolve_sealed_refuses_a_link_out_of_the_root(tmp_path: Path):
    """A stream directory that links elsewhere is not followed."""
    root = tmp_path / "root"
    elsewhere = make_recording(tmp_path / "elsewhere")
    shipper.pack(tmp_path / "elsewhere")
    (root / "md").mkdir(parents=True)
    (root / "md" / "link").symlink_to(elsewhere)

    with pytest.raises(ShipperError):
        shipper.resolve_sealed(root, "md/link/2026-09-06T00.jsonl.zst")


def test_send_writes_the_requested_files_as_tar(tmp_path: Path):
    """Each member is named by its path relative to the root."""
    make_recording(tmp_path)
    shipper.pack(tmp_path)
    names = [entry.path for entry in shipper.manifest(tmp_path)]
    out = io.BytesIO()

    assert shipper.send(tmp_path, names, out) == 2

    out.seek(0)
    with tarfile.open(fileobj=out, mode="r|") as tar:
        received = {member.name: tar.extractfile(member).read() for member in tar}
    assert received == {name: (tmp_path / name).read_bytes() for name in names}


def test_send_writes_nothing_when_one_name_is_refused(tmp_path: Path):
    """A bad request fails before the stream starts."""
    make_recording(tmp_path)
    shipper.pack(tmp_path)
    out = io.BytesIO()

    with pytest.raises(ShipperError):
        shipper.send(
            tmp_path,
            [f"{BOOK}/2026-09-06T00.jsonl.zst", f"{BOOK}/2026-09-06T02.jsonl"],
            out,
        )
    assert out.getvalue() == b""


def test_prune_deletes_only_files_whose_checksum_matches(tmp_path: Path):
    """Changed, missing and refused files are reported, not deleted."""
    make_recording(tmp_path)
    shipper.pack(tmp_path)
    first, second = shipper.manifest(tmp_path)

    results = shipper.prune(
        tmp_path,
        [
            PruneRequest(path=first.path, sha256=first.sha256),
            PruneRequest(path=second.path, sha256="0" * 64),
            PruneRequest(path=f"{BOOK}/2026-09-05T00.jsonl.zst", sha256="0" * 64),
            PruneRequest(path=f"{BOOK}/2026-09-06T02.jsonl", sha256="0" * 64),
        ],
    )

    assert [result.status for result in results] == [
        "pruned",
        "changed",
        "missing",
        "refused",
    ]
    assert not (tmp_path / first.path).exists()
    assert (tmp_path / second.path).exists()
    assert (tmp_path / BOOK / "2026-09-06T02.jsonl").exists()


def test_run_speaks_json_lines(tmp_path: Path):
    """Manifest packs first and prints one JSON object per line."""
    make_recording(tmp_path)
    out = io.BytesIO()

    shipper.run("manifest", tmp_path, io.BytesIO(), out)

    reply = out.getvalue().splitlines()
    assert len(reply) == 2
    assert reply[0].startswith(b'{"path":"md/book/venue_a/BASE-QUOTE/2026-09-06T00')


def test_run_refuses_an_unknown_verb(tmp_path: Path):
    """Nothing outside the four verbs runs."""
    with pytest.raises(ShipperError):
        shipper.run("serve", tmp_path, io.BytesIO(), io.BytesIO())


@pytest.mark.parametrize(
    ("command", "verb"),
    [
        ("manifest", "manifest"),
        ("cd the_bot && uv run -m apps.maker.src.shipper send", "send"),
        ("rm -rf / ; prune", "prune"),
    ],
)
def test_verb_of_ssh_command_takes_the_last_word(command: str, verb: str):
    """Under a forced command only the verb is honoured."""
    assert shipper.verb_of_ssh_command(command) == verb


def test_serve_without_a_command_fails(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """An interactive login under the forced command gets an error, not a shell."""
    monkeypatch.delenv("SSH_ORIGINAL_COMMAND", raising=False)

    assert shipper.main(["serve", "--root", str(tmp_path)]) == 2
