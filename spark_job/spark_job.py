from pyspark.sql import SparkSession
import argparse
import logging
from pyspark.sql.functions import col, count, avg, when, lit, expr
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging=logging.getLogger(__name__)

def main(env,bq_project,bq_dataset,transformed_table,route_insights_table,orgin_insights_table):
    try:
        spark=SparkSession.builder.appName("FlightBookingCICD").getOrCreate()
        logging.info("Spark session Intialized.")

        input_path= f"gs://spark_ex_airflow/flight_cicd/source-{env}"
        logging.info(f"Input path resolved for {env}")

        data=spark.read.csv(input_path,header=True,inferSchema=True)
        logging.info("Data read from GCS Bucket")

        logging.info("Transformation started..")

        transformed_data=data.withColumn(
            "is_weekend",when(col("flight_day").isin("Sat","Sun"),lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category",when(col("purchase_lead")<7,lit("Last-minute"))
                                .when((col("purchase_lead")>=7)& (col("purchase_lead")<30),lit("Short-Term"))
                                .otherwise("Long-term")
        ).withColumn(
            "booking_success_rate",expr("booking_complete"/"num_passenger")
        )

        route_data=transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("Avg Flight Duration"),
            avg("length_of_stay").alias("Avg_Stay_Length")
        )

        booking_origin_insights=transformed_data.groupBy("booking_orgin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("Avg purchase lead")
        )

        logging.info("Data Tranformations completed.")

        logging.info(f"Writing transformed data into BigQuery Table: {bq_project}:{bq_dataset}.{transformed_table}")
        transformed_data.write\
            .format("bigquery")\
            .option("table",f"{bq_project}:{bq_dataset}.{transformed_table}")\
            .option("writeMethod","direct")\
            .mode("overwrite")\
            .save()
        
        logging.info(f"Writing transformed data into BigQuery Table: {bq_project}:{bq_dataset}.{route_insights_table}")
        route_data.write\
            .format("bigquery")\
            .option("table",f"{bq_project}:{bq_dataset}.{route_insights_table}")\
            .option("writeMethod","direct")\
            .mode("overwrite")\
            .save()
        
        logging.info(f"Writing transformed data into BigQuery Table: {bq_project}:{bq_dataset}.{orgin_insights_table}")
        booking_origin_insights.write\
            .format("bigquery")\
            .option("table",f"{bq_project}:{bq_dataset}.{orgin_insights_table}")\
            .option("writeMethod","direct")\
            .mode("overwrite")\
            .save()
            
        logging.info("Data written to BigQuery Successfully!")

    except Exception as e:
         logging.error(f"An Error occurred:{e}")
         sys.exit(1)
         
    finally:
          spark.stop()
          logging.info("Spark session stopped.")

if __name__== "__main__":
    parser=argparse.ArgumentParser(description="Process flight booking data and write to BigQuery")
    parser.add_argument("--env",required=True, help="Environment(ex: dev, prod)")
    parser.add_argument("--bq_project",required=True, help="BigQuery Project ID")
    parser.add_argument("--bq_dataset",required=True, help="BigQuery Dataset")
    parser.add_argument("--transformed_table",required=True, help="BigQuery Table for transformed data")
    parser.add_argument("--route_insights_table",required=True, help="BigQuery Table for Route Insights data")
    parser.add_argument("--orgin_insights_table",required=True, help="BigQuery Table for Origin Insights data")

    args=parser.parse_args()

    main(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        orgin_insights_table=args.orgin_insights_table
    )