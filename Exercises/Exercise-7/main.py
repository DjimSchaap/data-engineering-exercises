from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


def readHardDriveData(spark: SparkSession, archivePath: Path, extractionPath: Path) -> DataFrame:
    with ZipFile(archivePath) as archive:
        csvEntry = next(entry for entry in archive.infolist() if not entry.is_dir() and entry.filename.endswith('.csv') and not entry.filename.startswith('__MACOSX/'))
        archive.extract(csvEntry, extractionPath)

    return spark.read.option('header', True).option('inferSchema', True).csv(str(extractionPath / csvEntry.filename))


def addSourceColumns(dataFrame: DataFrame) -> DataFrame:
    return dataFrame.withColumn('source_file', F.input_file_name()).withColumn(
        'file_date',
        F.to_date(F.regexp_extract(F.col('source_file'), r'(\d{4}-\d{2}-\d{2})', 1)),
    )


def addBrand(dataFrame: DataFrame) -> DataFrame:
    return dataFrame.withColumn(
        'brand',
        F.when(F.instr(F.col('model'), ' ') > 0, F.split(F.col('model'), ' ').getItem(0)).otherwise(F.lit('unknown')),
    )


def createStorageRankings(dataFrame: DataFrame) -> DataFrame:
    capacityByModel = dataFrame.groupBy('model').agg(F.max('capacity_bytes').alias('model_capacity_bytes'))

    return capacityByModel.withColumn(
        'storage_ranking',
        F.dense_rank().over(Window.orderBy(F.col('model_capacity_bytes').desc())),
    ).select('model', 'storage_ranking')


def addStorageRanking(dataFrame: DataFrame) -> DataFrame:
    return dataFrame.join(createStorageRankings(dataFrame), 'model', 'left')


def addPrimaryKey(dataFrame: DataFrame) -> DataFrame:
    return dataFrame.withColumn(
        'primary_key',
        F.sha2(F.concat_ws('||', F.col('date').cast('string'), F.col('serial_number')), 256),
    )


def transformHardDriveData(dataFrame: DataFrame) -> DataFrame:
    return addPrimaryKey(addStorageRanking(addBrand(addSourceColumns(dataFrame))))


def main() -> None:
    spark = SparkSession.builder.appName('Exercise7').enableHiveSupport().getOrCreate()

    try:
        archivePath = Path(__file__).parent / 'data' / 'hard-drive-2022-01-01-failures.csv.zip'

        with TemporaryDirectory() as temporaryDirectory:
            dataFrame = readHardDriveData(spark, archivePath, Path(temporaryDirectory))
            transformHardDriveData(dataFrame).show(truncate=False)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
