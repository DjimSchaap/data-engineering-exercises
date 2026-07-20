import polars as pl


def main():
    lf = pl.scan_csv(
        "data/202306-divvy-tripdata.csv",
        schema={
            "ride_id": pl.String,
            "rideable_type": pl.String,
            "started_at": pl.Datetime,
            "ended_at": pl.Datetime,
            "start_station_name": pl.String,
            "start_station_id": pl.String,
            "end_station_name": pl.String,
            "end_station_id": pl.String,
            "start_lat": pl.Float64,
            "start_lng": pl.Float64,
            "end_lat": pl.Float64,
            "end_lng": pl.Float64,
            "member_casual": pl.String,
        },
    )

    bike_rides_per_day = (
        lf.with_columns(day=pl.col("started_at").dt.date())
        .group_by("day")
        .len()
        .sort("day")
    )

    # print(bike_rides_per_day.collect())

    weekly_rides = (
        lf.sort("started_at")
        .group_by_dynamic("started_at", every="1w")
        .len(name="rides")
        .select(
            pl.col("rides").mean().alias("average_rides_per_week"),
            pl.col("rides").max().alias("max_rides_per_week"),
            pl.col("rides").min().alias("min_rides_per_week"),
        )
    )

    # print(weekly_rides.collect())

    daily_rides = (
        lf.with_columns(day=pl.col("started_at").dt.date())
        .group_by("day")
        .len(name="rides")
    )

    rides_vs_last_week = (
        daily_rides.with_columns(previous_week_day=pl.col("day") - pl.duration(days=7))
        .join(
            daily_rides.rename(
                {
                    "day": "previous_week_day",
                    "rides": "rides_last_week",
                }
            ),
            on="previous_week_day",
            how="left",
        )
        .with_columns(
            rides_difference=(
                pl.col("rides").cast(pl.Int64)
                - pl.col("rides_last_week").cast(pl.Int64)
            )
        )
        .select(
            "day",
            "rides",
            "rides_last_week",
            "rides_difference",
        )
        .sort("day")
    )

    print(rides_vs_last_week.collect())

    pass


if __name__ == "__main__":
    main()
