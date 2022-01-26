import pyspark.sql.functions as F
import pyspark.sql.types as T
from src.controller.data_controller import DataController
import datetime

class Runner:
    def __init__(self,spark,env,running_date,start_year):
        self.spark = spark
        self.env = env
        self.running_date = running_date
        self.start_year = start_year

    def run(self):
        # spark= self.spark
        start_year=self.start_year
        env_recsys = f"{self.env}_recsys"
        _DataController = DataController(self.spark, env_recsys,self.env)
        product_df = _DataController.get_product_info(start_year)
        running_date = self.running_date
        purchase_df = _DataController.get_purchase_data(running_date)
        # purchase_df.printSchema()
        purchase_df.printSchema()
        # product_purchase_df = purchase_df.join(product_df, ['product_id'])
        purchase_df = purchase_df.select('p1_date', 'hour', 'user_id', 'GMV', 'order_id',
                                                    F.struct('product_id', 'sales_unit').alias('product_detail'))
        purchase_summary_df = purchase_df.groupBy('p1_date', 'hour', 'user_id',  'order_id').agg(F.collect_list( 'product_detail').alias('order_product_detail'),F.sum('GMV').alias('order_GMV'))
        purchase_summary_df.printSchema()
        purchase_summary_df = purchase_summary_df.select('p1_date', 'hour', 'user_id', 'order_id','order_GMV',F.to_json(F.struct('order_id','order_product_detail')).alias('basket_product_detail'),
                                                         F.to_json(F.struct('order_id','order_GMV')).alias('basket_GMV_detail'))

        purchase_summary_df = purchase_summary_df.groupBy('p1_date', 'hour', 'user_id').agg(
            F.collect_set('basket_product_detail').alias('p1_product_list'),F.collect_set('basket_GMV_detail').alias('p1_GMV_list'),
             F.sum('order_GMV').alias('p1_GMV'))
        print(f"number of user is {purchase_summary_df.count()}")

        realtime_hits_df = _DataController.get_hit_data(running_date)
        hour = 18
        df_hist = _DataController.get_hist_result(running_date, hour)
        df_user_list_no_history = _DataController.get_user_list_no_history(running_date)
        recommender_user_list = (
            df_hist.select("user_id").unionByName(df_user_list_no_history.select("user_id"))).distinct()

        print(f"recommender list is {recommender_user_list.count()}")

        recommender_user_list = recommender_user_list.withColumn("bucket", F.substring(F.col("user_id"), -1, 1))
        recommender_user_list = recommender_user_list.withColumn('algo', F.when(F.col('bucket').isin(["0", '3']), "transition_model").otherwise('KGCN'))
        recommender_user_list = recommender_user_list.withColumn('p1_date',F.lit(running_date))
        # recommender_user_list = recommender_user_list.withColumn('group',F.lit('AB'))

        recommender_user_list.printSchema()
        print(recommender_user_list.count())

        result_df = realtime_hits_df.join(recommender_user_list, ['p1_date',"user_id"],'full')
        hit_by_user_impression = result_df.filter(F.col("hitType") == "ec:impression").groupBy('p1_date', "hour", 'algo', 'user_id',
                                                                                               ).agg( F.collect_list('product_id').alias('product_info_impression'))
        hit_by_user_click = result_df.filter(F.col("hitType") == "ec:click").groupBy('p1_date', "hour", 'algo', 'user_id',
                                                                                     ).agg( F.collect_list('product_id').alias('product_info_click'))
        hit_by_user = hit_by_user_impression.join(hit_by_user_click, ['p1_date', "hour", 'algo', 'user_id'], 'full')
        hit_by_user.printSchema()

        purchase_hit_df = purchase_summary_df.join( hit_by_user,['p1_date', "hour", 'user_id'],"full")
        purchase_hit_df.printSchema()
        print(f"num of user is {purchase_hit_df.count()}")
        # purchase_hit_df.groupBy().sum('p1_GMV').show()

        env = 'prod_recsys'
        table = f"ztore-data.{env}.user_purchase_history"
        history_df = self.spark.read.format("bigquery").option("table", table).load()

        history_df = history_df.filter(f"computation_date <='{running_date}'")
        latest_user_action_date_df = history_df.groupBy("user_id").agg(
            F.max("computation_date").alias("computation_date"))
        history_df = history_df.join(latest_user_action_date_df, ["user_id", "computation_date"])
        history_df = history_df.select("user_id", F.col("p1_date").alias("p2_date"))

        # add running_date info
        history_df = history_df.withColumn("p1_date", F.lit(running_date))
        history_df = history_df.withColumn("p1_date", F.col("p1_date").cast(T.DateType()))
        # add columns for matching inference input data
        history_df = history_df.withColumn("delta", F.datediff("p1_date", "p2_date"))
        history_df = history_df.withColumn("day_of_week", F.dayofweek("p1_date"))
        # history_df = history_df.withColumn('product_id', F.explode(F.col('p2_hours_products').products)).drop(
        #     'p2_hours_products').distinct()

        table_p2_purchase = _DataController.get_all_transaction()
        history_p1_p2 = history_df.join(table_p2_purchase,['p2_date','user_id']).select('user_id','p1_date',
                                                            'p2_date','delta','product_id','sales_unit','GMV_pre')
        history_p1_p2 = history_p1_p2.join(product_df,['product_id']).select('user_id','p1_date',
                                                            'p2_date','delta','GMV_pre',
                                                        F.to_json(F.struct('product_id', 'sales_unit')).alias('p2_product_info'))
        history_p1_p2_summary = history_p1_p2.groupBy('user_id', 'p2_date','delta').agg(F.collect_list('p2_product_info').alias('p2_product_info_list'),
                                                                                                 F.sum('GMV_pre').alias('p2_GMV'))
        df_final = purchase_hit_df.join(history_p1_p2_summary,['user_id'],'left')

        df_final = df_final.fillna('Other', ['algo'])
        df_final = df_final.dropDuplicates(['user_id','p1_date','hour']).orderBy('user_id','p1_date','hour')

        df_final.groupBy().sum('p1_GMV').show()
        df_final.printSchema()

        res_local = "D:/Work/Project/AB_testing/data"
        df_final.coalesce(50).write.mode("overwrite").partitionBy("p1_date", 'hour').save(res_local)





