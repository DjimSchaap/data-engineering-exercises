import csv
import json
from pathlib import Path
from typing import Any


def flatten(data: Any, prefix: str = "") -> dict[str, Any]:
    flattened = {}

    if isinstance(data, dict):
        for key, value in data.items():
            field = f"{prefix}_{key}" if prefix else key
            flattened.update(flatten(value, field))

        return flattened

    if isinstance(data, list):
        for index, value in enumerate(data):
            field = f"{prefix}_{index}" if prefix else str(index)
            flattened.update(flatten(value, field))

        return flattened

    flattened[prefix] = data

    return flattened


def convert(jsonPath: Path) -> Path:
    with jsonPath.open(encoding="utf-8") as jsonFile:
        data = json.load(jsonFile)

    records = data if isinstance(data, list) else [data]
    rows = [flatten(record) for record in records]
    headers = list(dict.fromkeys(key for row in rows for key in row))
    csvPath = jsonPath.with_suffix(".csv")

    with csvPath.open("w", encoding="utf-8", newline="") as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    return csvPath


def main() -> None:
    dataPath = Path(__file__).parent / "data"

    for jsonPath in dataPath.rglob("*.json"):
        convert(jsonPath)


if __name__ == "__main__":
    main()
