import gzip
import io
import os

import requests


def main():
    baseUrl = "https://data.commoncrawl.org"
    pathsKey = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    with requests.get(f"{baseUrl}/{pathsKey}", stream=True) as response:
        response.raise_for_status()
        response.raw.decode_content = True

        with gzip.GzipFile(fileobj=response.raw) as file:
            key = io.TextIOWrapper(file, encoding="utf-8").readline().strip()

    localFile = os.path.basename(key)

    with requests.get(f"{baseUrl}/{key}", stream=True) as response:
        response.raise_for_status()

        with open(localFile, mode="wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    with gzip.open(localFile, mode="rt", encoding="utf-8") as file:
        for line in file:
            print(line, end="")


if __name__ == "__main__":
    main()
