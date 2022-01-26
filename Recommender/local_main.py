import os
from src.runner import Runner
import argparse
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = "C:/Users/User/AppData/Local/Programs/Python/Python39"
os.environ['PYSPARK_DRIVER_PYTHON'] =  "C:/Users/User/AppData/Local/Programs/Python/Python39"

parser = argparse.ArgumentParser()
parser.add_argument("--env", help="running environment.", default="dev")
parser.add_argument("--running_date", help="running date.",default="2022-01-19")
parser.add_argument("--start_year", help="start year", default= 2021)
args = parser.parse_args()

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[18]') \
        .config("spark.jars", "../library/gcs-connector-hadoop2-latest.jar,../library/spark-bigquery-latest_2.12.jar") \
        .config("spark.driver.memory", "54g").config("spark.executor.memory", "32g").config("spark.executor.instances",
                                                                                            "16") \
        .config("spark.executor.heartbeatInterval", "3600s") \
        .config("spark.network.timeout", "36000s") \
        .appName('training_main') \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "ztore_site_click")

    _Runner = Runner(spark,args.env,args.running_date,args.start_year)
    _Runner.run()