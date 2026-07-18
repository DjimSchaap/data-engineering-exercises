from pathlib import Path

import duckdb


DATA_FILE = Path('data/Electric_Vehicle_Population_Data.csv')
OUTPUT_DIRECTORY = Path('output/electric_cars_by_model_year')


def createElectricVehiclesTable(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute('''
        CREATE OR REPLACE TABLE electric_vehicles (
            vin VARCHAR NOT NULL,
            county VARCHAR,
            city VARCHAR,
            state VARCHAR,
            postal_code VARCHAR,
            model_year SMALLINT,
            make VARCHAR,
            model VARCHAR,
            electric_vehicle_type VARCHAR,
            cafv_eligibility VARCHAR,
            electric_range SMALLINT,
            base_msrp INTEGER,
            legislative_district SMALLINT,
            dol_vehicle_id BIGINT,
            vehicle_location VARCHAR,
            electric_utility VARCHAR,
            census_tract VARCHAR
        )
    ''')


def loadElectricVehicles(
    connection: duckdb.DuckDBPyConnection,
    csvFile: Path = DATA_FILE,
) -> None:
    connection.execute(
        "COPY electric_vehicles FROM ? (FORMAT CSV, HEADER true)",
        [str(csvFile)],
    )


def countElectricCarsPerCity(
    connection: duckdb.DuckDBPyConnection,
) -> list[tuple[str, int]]:
    return connection.execute('''
        SELECT city, count(*) AS vehicle_count
        FROM electric_vehicles
        GROUP BY city
        ORDER BY vehicle_count DESC, city
    ''').fetchall()


def findTopElectricVehicles(
    connection: duckdb.DuckDBPyConnection,
    limit: int = 3,
) -> list[tuple[str, str, int]]:
    return connection.execute('''
        SELECT make, model, count(*) AS vehicle_count
        FROM electric_vehicles
        GROUP BY make, model
        ORDER BY vehicle_count DESC, make, model
        LIMIT ?
    ''', [limit]).fetchall()


def findMostPopularElectricVehiclePerPostalCode(
    connection: duckdb.DuckDBPyConnection,
) -> list[tuple[str, str, str, int]]:
    return connection.execute('''
        WITH vehicle_counts AS (
            SELECT postal_code, make, model, count(*) AS vehicle_count
            FROM electric_vehicles
            WHERE postal_code IS NOT NULL
            GROUP BY postal_code, make, model
        ),
        ranked_vehicles AS (
            SELECT *, row_number() OVER (
                PARTITION BY postal_code
                ORDER BY vehicle_count DESC, make, model
            ) AS popularity_rank
            FROM vehicle_counts
        )
        SELECT postal_code, make, model, vehicle_count
        FROM ranked_vehicles
        WHERE popularity_rank = 1
        ORDER BY postal_code
    ''').fetchall()


def writeElectricCarCountsByModelYear(
    connection: duckdb.DuckDBPyConnection,
    outputDirectory: Path = OUTPUT_DIRECTORY,
) -> None:
    outputDirectory.mkdir(parents=True, exist_ok=True)
    connection.execute(f'''
        COPY (
            SELECT model_year, count(*) AS vehicle_count
            FROM electric_vehicles
            WHERE model_year IS NOT NULL
            GROUP BY model_year
        )
        TO '{outputDirectory.as_posix()}' (
            FORMAT PARQUET,
            PARTITION_BY (model_year),
            OVERWRITE_OR_IGNORE true
        )
    ''')


def printResults(title: str, rows: list[tuple]) -> None:
    print(f'\n{title}')
    for row in rows:
        print(row)


def main() -> None:
    connection = duckdb.connect()
    createElectricVehiclesTable(connection)
    loadElectricVehicles(connection)

    printResults('Electric cars per city', countElectricCarsPerCity(connection))
    printResults('Top 3 electric vehicles', findTopElectricVehicles(connection))
    printResults(
        'Most popular electric vehicle per postal code',
        findMostPopularElectricVehiclePerPostalCode(connection),
    )
    writeElectricCarCountsByModelYear(connection)
    connection.close()


if __name__ == "__main__":
    main()
