from pathlib import Path
from zipfile import ZipFile

import pytest

from main import extract_zip, filename_from_uri


def test_filename_from_uri() -> None:
    assert filename_from_uri("https://example.com/files/Monkey%20Trips.zip?download=1") == "Monkey Trips.zip"


def test_filename_from_uri_requires_filename() -> None:
    with pytest.raises(ValueError):
        filename_from_uri("https://example.com/")


def test_extract_zip_extracts_csv_and_deletes_zip(tmp_path: Path) -> None:
    zip_path = tmp_path / "animals.zip"

    with ZipFile(zip_path, "w") as archive:
        archive.writestr("animals.csv", "animal\nmonkey\n")
        archive.writestr("__MACOSX/._animals.csv", "metadata")
        archive.writestr("readme.txt", "not a csv")

    extract_zip(zip_path, tmp_path)

    assert (tmp_path / "animals.csv").read_text() == "animal\nmonkey\n"
    assert not (tmp_path / "__MACOSX").exists()
    assert not (tmp_path / "readme.txt").exists()
    assert not zip_path.exists()


def test_extract_zip_rejects_unsafe_paths(tmp_path: Path) -> None:
    zip_path = tmp_path / "animals.zip"

    with ZipFile(zip_path, "w") as archive:
        archive.writestr("../animals.csv", "animal\ntapir\n")

    with pytest.raises(ValueError, match="Unsafe path"):
        extract_zip(zip_path, tmp_path)

    assert zip_path.exists()
