import os
from src.runner import Runner
import argparse
from pyspark.sql import SparkSession



parser = argparse.ArgumentParser()
parser.add_argument("--env", help="running environment." , default="dev")
parser.add_argument("--running_date", help="running date.")
parser.add_argument("--hour", help="running date.")
args = parser.parse_args()

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    _Runner = Runner(spark,args.env,args.running_date,args.hour)
    _Runner.run()