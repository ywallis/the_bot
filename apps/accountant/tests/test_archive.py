"""Tests for the archive host, pulling from a local shipper process."""

import io
import sqlite3
from pathlib import Path

import pytest

from apps.accountant.src import archive
from apps.accountant.src.archive import ArchiveError
from apps.maker.src.shipper import ShippedFile
from apps.shared.src.config import (
    ArchiveConfig,
    ArchiveSource,
    ConfigError,
    parse_app_config,
)
from apps.shared.src.streams import open_recording, recording_files

BOOK = "md/book/venue_a/BASE-QUOTE"
TRADE = "md/trade/venue_b/BASE-QUOTE"
BUCKETS = ("2026-09-06T00", "2026-09-06T01", "2026-09-06T02")


def lines(stream: str, bucket: str) -> bytes:
    """Return the content of a small recording."""
    return b"".join(
        f'{{"id":"{i}-0","type":"book","data":{{"s":"{stream}","b":"{bucket}"}}}}\n'.encode()
        for i in range(200)
    )


@pytest.fixture
def setup(tmp_path: Path) -> tuple[Path, ArchiveConfig]:
    """Return a source recording root and archive settings pulling from it."""
    source_root = tmp_path / "source"
    for stream in (BOOK, TRADE):
        directory = source_root / stream
        directory.mkdir(parents=True)
        for bucket in BUCKETS:
            (directory / f"{bucket}.jsonl").write_bytes(lines(stream, bucket))
    settings = ArchiveConfig(
        root=str(tmp_path / "archive"),
        sources=(ArchiveSource(name="host_a", root=str(source_root)),),
    )
    return source_root, settings


def ledger_rows(settings: ArchiveConfig) -> list[tuple]:
    """Return (path, pruned) for every ledger row."""
    with sqlite3.connect(Path(settings.root) / archive.LEDGER_NAME) as connection:
        return connection.execute(
            "SELECT path, pruned_at IS NOT NULL FROM files ORDER BY path"
        ).fetchall()


def test_pull_archives_sealed_files_and_prunes_the_source(setup):
    """Sealed files move to the archive, recompressed; live files stay."""
    source_root, settings = setup

    (report,) = archive.run(settings, [], prune=True)

    assert report.listed == 4
    assert len(report.archived) == 4
    assert len(report.pruned) == 4
    assert report.failed == []
    for stream in (BOOK, TRADE):
        kept = [path.name for path in recording_files(source_root / stream)]
        assert kept == ["2026-09-06T02.jsonl"]
        stored = recording_files(Path(settings.root) / "host_a" / stream)
        assert [path.name for path in stored] == [
            "2026-09-06T00.jsonl.zst",
            "2026-09-06T01.jsonl.zst",
        ]
        for path in stored:
            with open_recording(path) as f:
                assert f.read() == lines(stream, path.name[:13])
    assert [pruned for _, pruned in ledger_rows(settings)] == [1, 1, 1, 1]


def test_a_second_pull_fetches_nothing(setup):
    """Archived and pruned files are not listed again."""
    _, settings = setup
    archive.run(settings, [], prune=True)

    (report,) = archive.run(settings, [], prune=True)

    assert (report.listed, report.archived, report.pruned) == (0, [], [])


def test_without_prune_the_source_keeps_its_files_until_a_later_run(setup):
    """A later pruning run deletes what is archived without fetching again."""
    source_root, settings = setup

    (first,) = archive.run(settings, [], prune=False)
    assert len(first.archived) == 4 and first.pruned == []
    assert len(recording_files(source_root / BOOK)) == 3

    (second,) = archive.run(settings, [], prune=True)
    assert second.archived == []
    assert len(second.pruned) == 4
    assert len(recording_files(source_root / BOOK)) == 1


def test_a_file_changed_on_the_source_is_archived_again(setup):
    """The ledger matches on checksum, not on the path alone."""
    source_root, settings = setup
    archive.run(settings, [], prune=False)
    changed = source_root / BOOK / "2026-09-06T00.jsonl.zst"
    changed.unlink()
    (source_root / BOOK / "2026-09-06T00.jsonl").write_bytes(b'{"id":"9-0"}\n')

    (report,) = archive.run(settings, [], prune=False)

    assert report.archived == [f"{BOOK}/2026-09-06T00.jsonl.zst"]
    stored = Path(settings.root) / "host_a" / BOOK / "2026-09-06T00.jsonl.zst"
    with open_recording(stored) as f:
        assert f.read() == b'{"id":"9-0"}\n'


@pytest.mark.parametrize(
    "path",
    ["../escape.jsonl.zst", "/abs/2026.jsonl.zst", "md//x.jsonl.zst", "md/x.jsonl"],
)
def test_an_unsafe_manifest_path_stops_the_pull(monkeypatch, setup, path: str):
    """A compromised source cannot make the archive write outside its root."""
    _, settings = setup
    reply = f'{{"path":"{path}","size":1,"sha256":"{"0" * 64}"}}\n'.encode()
    monkeypatch.setattr(archive, "_call", lambda *_args, **_kw: reply)

    with pytest.raises(ArchiveError):
        archive.fetch_manifest(settings.sources[0])


def test_receive_rejects_a_file_that_does_not_match(tmp_path: Path):
    """A transfer is kept only if size and checksum match the manifest."""
    expected = ShippedFile(path="a.jsonl.zst", size=3, sha256="0" * 64)

    assert archive._receive(io.BytesIO(b"abc"), tmp_path / "x", expected) is False


def test_an_unknown_source_is_an_error(setup):
    """A typo in --source does not silently pull nothing."""
    _, settings = setup

    with pytest.raises(ArchiveError):
        archive.run(settings, ["host_b"], prune=False)


def test_ssh_command_appends_the_verb():
    """The remote command and the verb travel as one SSH command string."""
    source = ArchiveSource(name="host_a", ssh="host-alias", command="run-shipper")

    assert archive.shipper_command(source, "manifest") == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "host-alias",
        "run-shipper manifest",
    ]


@pytest.mark.parametrize(
    "sources",
    [
        [{"name": "a"}, {"name": "a"}],
        [{"name": "../a"}],
        [{"name": ".hidden"}],
        [{"name": ""}],
    ],
)
def test_archive_source_names_must_be_plain_and_unique(sources):
    """A source name becomes a directory, so it is checked like one."""
    raw = {
        "venues": [{"id": "venue_a", "name": "Venue A"}],
        "strategies": [
            {
                "identifier": "s",
                "module": "m",
                "production": False,
                "subscriptions": [],
            }
        ],
        "archive": {"sources": sources},
    }

    with pytest.raises(ConfigError):
        parse_app_config(raw)
