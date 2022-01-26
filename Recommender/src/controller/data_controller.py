import pyspark.sql.functions as F

class DataController:

    def __init__(self,spark,env_recsys,env):
        self.spark = spark
        self.env = env
        self.env_recsys = env_recsys

    def get_all_transaction(self):

        table_p2_purchase = "datawarehouse.ztore_order_product_summary"
        p2_product_query = f"""
        SELECT user_id, product_id, sum(sales_unit) as sales_unit,sum(sales_value) as GMV_pre, date(order_date) as p2_date
         FROM {table_p2_purchase} WHERE DATE(order_date) >= "2020-01-01" 
        group by user_id, product_id, p2_date
        order by p2_date, user_id, product_id
        """
        p2_purchase_df = self.spark.read.format("bigquery").load(p2_product_query)
        return p2_purchase_df

    def get_user_list_no_history(self,running_date):

        df = self.spark.read.load(f"gs://{self.env_recsys}/prediction/no_history_user_id")
        df = df.filter(f"p1_date =='{running_date}'")
        return df

    def get_product_info(self,start_year):
        table_product = "datawarehouse.ztore_product_info_historical_daily"
        product_query = f"""
        SELECT product_id, ARRAY_AGG( DISTINCT product_name IGNORE NULLS) as prod_name_list,  ARRAY_AGG( distinct default_category.category IGNORE NULLS) as category_list, 
        FROM {table_product} 
        where extract(year from data_date) >= {start_year}
        group by product_id
        """
        product_df = self.spark.read.format("bigquery").load(product_query)
        product_df = product_df.withColumn('product_name',F.slice(F.col('prod_name_list'),1,1)).drop('prod_name_list')

        # product_df = product_df.select('product_id', F.to_json(F.struct('product_id', 'product_name', 'category_list')).alias('product_info'))

        return product_df

    def get_hit_data(self,running_date):
        table_hit = "ztore_site_click.realtime_hits"
        hit_query = f"""
        SELECT userId as user_id,productId as product_id,hitType,productListType,productListName,productPosition, mod(EXTRACT(HOUR FROM timestamp) +8,24) AS hour, 
        FROM {table_hit} WHERE DATE(TIMESTAMP_ADD(timestamp, INTERVAL 8 hour )) = "{running_date}" and
        userId is not null and
        productListType = 'slot' and
        productListName = 'recommender_user_product'
        """
        realtime_hits_df = self.spark.read.format("bigquery").load(hit_query)
        realtime_hits_df = realtime_hits_df.withColumn('p1_date', F.lit(running_date))

        return realtime_hits_df

    def get_user_purchase_history(self,running_date):

        table = "prod_recsys.user_purchase_history"
        history_df = self.spark.read.format("bigquery").option("table", table).load()

        history_df = history_df.filter(f"computation_date <='{running_date}'")
        latest_user_action_date_df = history_df.groupBy("user_id").agg(
            F.max("computation_date").alias("computation_date"))
        history_df = history_df.join(latest_user_action_date_df, ["user_id", "computation_date"])

        # add running_date info
        history_df = history_df.withColumn("p1_date", F.lit(running_date))
        history_df = history_df.withColumn("p1_date", F.col("p1_date").cast(T.DateType()))
        # add columns for matching inference input data
        history_df = history_df.withColumn("delta", F.datediff("p1_date", "p2_date"))
        history_df = history_df.withColumn("day_of_week", F.dayofweek("p1_date"))

        # add p1 empty list
        history_df = history_df.withColumn("p1_index_products", F.array())

        return history_df

    def get_purchase_data(self, running_date):
        table_product_purchase = "datawarehouse.ztore_order_product_summary"
        product_purchase_query = f"""
        
        SELECT order_id, date(order_date) as p1_date,extract(hour from order_date) as hour, user_id,product_id,
        sum(sales_value) as GMV, sum(sales_unit) as sales_unit
        FROM {table_product_purchase} WHERE DATE(order_date) = "{running_date}" 
        group by p1_date, order_id, hour, user_id,product_id
        
         """
        product_purchase_df = self.spark.read.format("bigquery").load(product_purchase_query)

        return product_purchase_df

    def get_hist_result(self,running_date,hour):
        df = self.spark.read.load(f"gs://{self.env_recsys}/prediction/history_inference")
        # df = df.withColumn("p1_date_str", F.date_format(F.col('p1_date')))
        # df = df.filter(F.col("p1_date") == running_date)
        df = df.filter(f"p1_date =='{running_date}'")
        df = df.filter(f"p1_hour =={hour}")
        return df