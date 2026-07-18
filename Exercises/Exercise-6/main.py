from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import List
from zipfile import ZipFile

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as functions
from pyspark.sql.window import Window


DATA_DIRECTORY = Path("data")
REPORT_DIRECTORY = Path("reports")


def read_trip_data(spark: SparkSession, dataDirectory: Path) -> DataFrame:
    tripDataFrames: List[DataFrame] = []

    with TemporaryDirectory() as temporaryDirectory:
        for archivePath in sorted(dataDirectory.glob("*.zip")):
            with ZipFile(archivePath) as archive:
                csvNames = [
                    name
                    for name in archive.namelist()
                    if name.lower().endswith(".csv") and not Path(name).name.startswith(".")
                ]

                for csvName in csvNames:
                    archive.extract(csvName, temporaryDirectory)
                    source = spark.read.option("header", True).option("inferSchema", True).csv(
                        str(Path(temporaryDirectory, csvName))
                    )
                    tripDataFrames.append(normalize_trip_data(source))

        trips = tripDataFrames[0]
        for tripDataFrame in tripDataFrames[1:]:
            trips = trips.unionByName(tripDataFrame)

        trips.cache()
        trips.count()

    return trips


def normalize_trip_data(trips: DataFrame) -> DataFrame:
    if "tripduration" in trips.columns:
        return trips.select(
            functions.to_date("start_time").alias("trip_date"),
            functions.col("tripduration").cast("double").alias("trip_duration_seconds"),
            functions.col("from_station_name").alias("start_station_name"),
            functions.col("gender"),
            (
                functions.year("start_time") - functions.col("birthyear")
            ).cast("integer").alias("age"),
        )

    return trips.select(
        functions.to_date("started_at").alias("trip_date"),
        (
            functions.unix_timestamp("ended_at") - functions.unix_timestamp("started_at")
        ).cast("double").alias("trip_duration_seconds"),
        functions.col("start_station_name"),
        functions.lit(None).cast("string").alias("gender"),
        functions.lit(None).cast("integer").alias("age"),
    )


def create_average_trip_duration_per_day_report(trips: DataFrame) -> DataFrame:
    report = trips.where(
        functions.col("trip_date").isNotNull()
        & functions.col("trip_duration_seconds").isNotNull()
        & (functions.col("trip_duration_seconds") >= 0)
    ).groupBy("trip_date").agg(
        functions.round(functions.avg("trip_duration_seconds"), 2).alias(
            "average_trip_duration_seconds"
        )
    ).orderBy("trip_date")

    write_csv_report(report, REPORT_DIRECTORY / "average_trip_duration_per_day.csv")

    return report


def create_trips_per_day_report(trips: DataFrame) -> DataFrame:
    report = trips.where(functions.col("trip_date").isNotNull()).groupBy("trip_date").agg(
        functions.count("*").alias("trip_count")
    ).orderBy("trip_date")

    write_csv_report(report, REPORT_DIRECTORY / "trips_per_day.csv")

    return report


def create_most_popular_start_station_per_month_report(trips: DataFrame) -> DataFrame:
    stationCounts = trips.where(
        functions.col("trip_date").isNotNull()
        & functions.col("start_station_name").isNotNull()
    ).groupBy(
        functions.date_format("trip_date", "yyyy-MM").alias("month"),
        "start_station_name",
    ).agg(
        functions.count("*").alias("trip_count")
    )
    ranking = Window.partitionBy("month").orderBy(
        functions.desc("trip_count"),
        functions.asc("start_station_name"),
    )
    report = stationCounts.withColumn(
        "station_rank", functions.row_number().over(ranking)
    ).where(
        functions.col("station_rank") == 1
    ).drop(
        "station_rank"
    ).orderBy(
        "month"
    )

    write_csv_report(report, REPORT_DIRECTORY / "most_popular_start_station_per_month.csv")

    return report


def create_top_three_start_stations_per_day_last_two_weeks_report(
    trips: DataFrame,
) -> DataFrame:
    latestDate = trips.agg(functions.max("trip_date").alias("latest_date")).first().latest_date
    stationCounts = trips.where(
        functions.col("trip_date").between(
            functions.date_sub(functions.lit(latestDate), 13),
            functions.lit(latestDate),
        )
        & functions.col("start_station_name").isNotNull()
    ).groupBy(
        "trip_date",
        "start_station_name",
    ).agg(
        functions.count("*").alias("trip_count")
    )
    ranking = Window.partitionBy("trip_date").orderBy(
        functions.desc("trip_count"),
        functions.asc("start_station_name"),
    )
    report = stationCounts.withColumn(
        "station_rank", functions.row_number().over(ranking)
    ).where(
        functions.col("station_rank") <= 3
    ).orderBy(
        "trip_date",
        "station_rank",
    )

    write_csv_report(
        report,
        REPORT_DIRECTORY / "top_three_start_stations_per_day_last_two_weeks.csv",
    )

    return report


def create_average_trip_duration_per_gender_report(trips: DataFrame) -> DataFrame:
    report = trips.where(
        functions.lower("gender").isin("male", "female")
        & functions.col("trip_duration_seconds").isNotNull()
        & (functions.col("trip_duration_seconds") >= 0)
    ).groupBy(
        functions.initcap(functions.lower("gender")).alias("gender")
    ).agg(
        functions.round(functions.avg("trip_duration_seconds"), 2).alias(
            "average_trip_duration_seconds"
        ),
        functions.count("*").alias("trip_count"),
    ).orderBy(
        functions.desc("average_trip_duration_seconds"),
        "gender",
    )

    write_csv_report(report, REPORT_DIRECTORY / "average_trip_duration_per_gender.csv")

    return report


def create_longest_and_shortest_trip_ages_report(trips: DataFrame) -> DataFrame:
    ageDurations = trips.where(
        functions.col("age").isNotNull()
        & functions.col("trip_duration_seconds").isNotNull()
        & (functions.col("trip_duration_seconds") >= 0)
    ).groupBy(
        "age"
    ).agg(
        functions.round(functions.avg("trip_duration_seconds"), 2).alias(
            "average_trip_duration_seconds"
        ),
        functions.count("*").alias("trip_count"),
    )
    longest = ageDurations.orderBy(
        functions.desc("average_trip_duration_seconds"),
        "age",
    ).limit(10).withColumn(
        "report_type", functions.lit("longest")
    )
    shortest = ageDurations.orderBy(
        "average_trip_duration_seconds",
        "age",
    ).limit(10).withColumn(
        "report_type", functions.lit("shortest")
    )
    ranking = Window.partitionBy("report_type").orderBy(
        functions.when(
            functions.col("report_type") == "longest",
            -functions.col("average_trip_duration_seconds"),
        ).otherwise(functions.col("average_trip_duration_seconds")),
        "age",
    )
    report = longest.unionByName(shortest).withColumn(
        "age_rank", functions.row_number().over(ranking)
    ).select(
        "report_type",
        "age_rank",
        "age",
        "average_trip_duration_seconds",
        "trip_count",
    ).orderBy(
        "report_type",
        "age_rank",
    )

    write_csv_report(report, REPORT_DIRECTORY / "longest_and_shortest_trip_ages.csv")

    return report


def write_csv_report(report: DataFrame, outputPath: Path) -> None:
    temporaryOutputPath = outputPath.with_name(f".{outputPath.name}.spark")
    report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(temporaryOutputPath)
    )


def main() -> None:
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    try:
        trips = read_trip_data(spark, DATA_DIRECTORY)
        create_average_trip_duration_per_day_report(trips)
        create_trips_per_day_report(trips)
        create_most_popular_start_station_per_month_report(trips)
        create_top_three_start_stations_per_day_last_two_weeks_report(trips)
        create_average_trip_duration_per_gender_report(trips)
        create_longest_and_shortest_trip_ages_report(trips)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
