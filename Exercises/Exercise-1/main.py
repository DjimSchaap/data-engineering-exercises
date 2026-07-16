import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse
from zipfile import ZipFile

import aiohttp


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def filename_from_uri(uri: str) -> str:
    filename = Path(unquote(urlparse(uri).path)).name

    if not filename:
        raise ValueError(f"URI does not contain a filename: {uri}")

    return filename


async def download_file(session: aiohttp.ClientSession, uri: str, downloads_directory: Path) -> Path:
    destination = downloads_directory / filename_from_uri(uri)
    temporary_destination = destination.with_suffix(f"{destination.suffix}.part")

    try:
        async with session.get(uri) as response:
            response.raise_for_status()

            with temporary_destination.open("wb") as download:
                async for chunk in response.content.iter_chunked(1024 * 1024):
                    download.write(chunk)

        temporary_destination.replace(destination)
    except Exception:
        temporary_destination.unlink(missing_ok=True)
        raise

    return destination


def extract_zip(zip_path: Path, downloads_directory: Path) -> None:
    downloads_root = downloads_directory.resolve()

    with ZipFile(zip_path) as archive:
        csv_members = [
            member
            for member in archive.infolist()
            if not member.is_dir()
            and Path(member.filename).suffix.lower() == ".csv"
            and "__MACOSX" not in PurePosixPath(member.filename).parts
        ]

        for member in csv_members:
            member_destination = (downloads_directory / member.filename).resolve()

            if not member_destination.is_relative_to(downloads_root):
                raise ValueError(f"Unsafe path in archive: {member.filename}")

            archive.extract(member, downloads_directory)

    zip_path.unlink()


async def download_and_extract_files(uris: list[str], downloads_directory: Path) -> list[tuple[str, Exception]]:
    downloads_directory.mkdir(parents=True, exist_ok=True)
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        downloads = await asyncio.gather(
            *(download_file(session, uri, downloads_directory) for uri in uris),
            return_exceptions=True,
        )

    failures = [(uri, result) for uri, result in zip(uris, downloads) if isinstance(result, Exception)]
    zip_paths = [result for result in downloads if isinstance(result, Path)]

    with ThreadPoolExecutor() as executor:
        extractions = await asyncio.gather(
            *(
                asyncio.get_running_loop().run_in_executor(
                    executor,
                    extract_zip,
                    zip_path,
                    downloads_directory,
                )
                for zip_path in zip_paths
            ),
            return_exceptions=True,
        )

    failures.extend(
        (str(zip_path), result)
        for zip_path, result in zip(zip_paths, extractions)
        if isinstance(result, Exception)
    )

    return failures


def main() -> None:
    downloads_directory = Path(__file__).resolve().parent / "downloads"
    failures = asyncio.run(download_and_extract_files(download_uris, downloads_directory))

    for uri, exception in failures:
        print(f"Failed to process {uri}: {exception}")

    print(f"Processed {len(download_uris) - len(failures)} of {len(download_uris)} files")


if __name__ == "__main__":
    main()
