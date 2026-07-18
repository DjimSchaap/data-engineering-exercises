from datetime import date

import pytest
from pyspark.sql import SparkSession

import main


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[2]").appName("Exercise6Tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def trips(spark: SparkSession):
    return spark.createDataFrame(
        [
            (date(2020, 1, 1), 100.0, "Alpha", "Male", 20),
            (date(2020, 1, 1), 200.0, "Alpha", "Female", 30),
            (date(2020, 1, 1), 300.0, "Beta", "Male", 40),
            (date(2020, 2, 1), 400.0, "Beta", "Female", 30),
            (date(2020, 2, 1), 500.0, "Beta", "Male", 50),
            (date(2020, 2, 16), 600.0, "Alpha", "Female", 60),
            (date(2020, 2, 16), 700.0, "Alpha", "Male", 70),
            (date(2020, 2, 16), 800.0, "Alpha", "Female", 80),
            (date(2020, 2, 16), 900.0, "Beta", "Male", 90),
            (date(2020, 2, 16), 1000.0, "Beta", "Female", 100),
            (date(2020, 2, 16), 1100.0, "Delta", "Male", 110),
            (date(2020, 2, 16), 1200.0, "Gamma", "Female", 120),
        ],
        [
            "trip_date",
            "trip_duration_seconds",
            "start_station_name",
            "gender",
            "age",
        ],
    )


@pytest.fixture(autouse=True)
def report_directory(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "REPORT_DIRECTORY", tmp_path)


def test_daily_reports(trips) -> None:
    averages = main.create_average_trip_duration_per_day_report(trips).collect()
    counts = main.create_trips_per_day_report(trips).collect()

    assert averages[0].trip_date == date(2020, 1, 1)
    assert averages[0].average_trip_duration_seconds == 200.0
    assert counts[0].trip_count == 3
    assert counts[-1].trip_count == 7


def test_most_popular_start_station_per_month(trips) -> None:
    report = main.create_most_popular_start_station_per_month_report(trips).collect()

    assert [(row.month, row.start_station_name, row.trip_count) for row in report] == [
        ("2020-01", "Alpha", 2),
        ("2020-02", "Beta", 4),
    ]


def test_top_three_stations_use_last_fourteen_days(trips) -> None:
    report = main.create_top_three_start_stations_per_day_last_two_weeks_report(
        trips
    ).collect()

    assert [row.start_station_name for row in report] == ["Alpha", "Beta", "Delta"]
    assert [row.station_rank for row in report] == [1, 2, 3]


def test_average_trip_duration_per_gender(trips) -> None:
    report = main.create_average_trip_duration_per_gender_report(trips).collect()
    averages = {row.gender: row.average_trip_duration_seconds for row in report}

    assert averages == {"Female": 700.0, "Male": 600.0}


def test_longest_and_shortest_trip_ages(trips) -> None:
    report = main.create_longest_and_shortest_trip_ages_report(trips).collect()
    longest = [row for row in report if row.report_type == "longest"]
    shortest = [row for row in report if row.report_type == "shortest"]

    assert len(longest) == 10
    assert len(shortest) == 10
    assert longest[0].age == 120
    assert shortest[0].age == 20
    assert [row.age_rank for row in longest] == list(range(1, 11))


def test_normalize_legacy_trip_data(spark: SparkSession) -> None:
    source = spark.createDataFrame(
        [
            (
                "2019-10-01 00:01:39",
                940.0,
                "Monkey Station",
                "Male",
                1987,
            )
        ],
        [
            "start_time",
            "tripduration",
            "from_station_name",
            "gender",
            "birthyear",
        ],
    )
    trip = main.normalize_trip_data(source).first()

    assert trip.trip_date == date(2019, 10, 1)
    assert trip.trip_duration_seconds == 940.0
    assert trip.start_station_name == "Monkey Station"
    assert trip.gender == "Male"
    assert trip.age == 32
