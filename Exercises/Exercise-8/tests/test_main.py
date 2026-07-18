from pathlib import Path

import duckdb

from main import (
    countElectricCarsPerCity,
    createElectricVehiclesTable,
    findMostPopularElectricVehiclePerPostalCode,
    findTopElectricVehicles,
    loadElectricVehicles,
    writeElectricCarCountsByModelYear,
)


CSV_HEADER = 'VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract'
CSV_ROWS = [
    'MONKEY0001,King,Seattle,WA,98101,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Eligible,250,0,43,1,POINT (-122.3 47.6),UTILITY,53033000100',
    'MONKEY0002,King,Seattle,WA,98101,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Eligible,250,0,43,2,POINT (-122.3 47.6),UTILITY,53033000100',
    'MONKEY0003,Pierce,Tacoma,WA,98402,2021,NISSAN,LEAF,Battery Electric Vehicle (BEV),Eligible,150,0,27,3,POINT (-122.4 47.2),UTILITY,53053000100',
    'MONKEY0004,King,Bellevue,WA,98101,2021,FORD,MUSTANG MACH-E,Battery Electric Vehicle (BEV),Eligible,230,0,48,4,POINT (-122.2 47.6),UTILITY,53033000200',
]


def createConnection(tmpPath: Path) -> duckdb.DuckDBPyConnection:
    csvFile = tmpPath / 'electric-cars.csv'
    csvFile.write_text('\n'.join([CSV_HEADER, *CSV_ROWS]), encoding='utf-8')
    connection = duckdb.connect()
    createElectricVehiclesTable(connection)
    loadElectricVehicles(connection, csvFile)
    return connection


def testAnalytics(tmp_path: Path) -> None:
    connection = createConnection(tmp_path)

    assert countElectricCarsPerCity(connection) == [
        ('Seattle', 2),
        ('Bellevue', 1),
        ('Tacoma', 1),
    ]
    assert findTopElectricVehicles(connection) == [
        ('TESLA', 'MODEL 3', 2),
        ('FORD', 'MUSTANG MACH-E', 1),
        ('NISSAN', 'LEAF', 1),
    ]
    assert findMostPopularElectricVehiclePerPostalCode(connection) == [
        ('98101', 'TESLA', 'MODEL 3', 2),
        ('98402', 'NISSAN', 'LEAF', 1),
    ]


def testWritesCountsAsPartitionedParquet(tmp_path: Path) -> None:
    connection = createConnection(tmp_path)
    outputDirectory = tmp_path / 'counts'

    writeElectricCarCountsByModelYear(connection, outputDirectory)

    assert connection.execute(
        "SELECT model_year, vehicle_count FROM read_parquet(?) ORDER BY model_year",
        [str(outputDirectory / '**/*.parquet')],
    ).fetchall() == [(2020, 2), (2021, 2)]
    assert (outputDirectory / 'model_year=2020').is_dir()
    assert (outputDirectory / 'model_year=2021').is_dir()
