from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
import os
import sys

def main():
    print("Starting Spark Analytics...")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("StrokeAnalyticsCache") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", 
               "org.apache.spark:spark-avro_2.12:3.5.1,"
               "org.apache.hadoop:hadoop-aws:3.3.4,"
               "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
               "org.postgresql:postgresql:42.5.0") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # PostgreSQL configuration
    postgres_url = "jdbc:postgresql://postgres:5432/analytics_db"
    postgres_properties = {
        "user": "analytics_user",
        "password": "analytics_pass",
        "driver": "org.postgresql.Driver"
    }

    try:
        print("Reading Avro data from S3...")
        # Read Avro files from S3
        input_path = "s3a://mlops-dbz-sink/topics/dbserver1.stroke_predictions.predictions/"
        df = spark.read.format("avro").load(input_path)
        print(f"✅ Successfully read {df.count()} records from S3")

        # Process data
        after_df = df.filter(df.after.isNotNull()).select(
            df.after.id.alias("id"),
            df.after.age.alias("age"),
            df.after.bmi.alias("bmi"),
            df.after.gender.alias("gender"),
            df.after.hypertension.alias("hypertension"),
            df.after.heart_disease.alias("heart_disease"),
            df.after.avg_glucose_level.alias("avg_glucose_level"),
            df.after.probability.alias("probability"),
            df.after.risk_category.alias("risk_category"),
            df.after.country.alias("country"),
            df.after.province.alias("province"),
            df.ts_ms
        )

        # Get latest predictions per ID
        window = Window.partitionBy("id").orderBy(F.col("ts_ms").desc())
        latest_predictions = (
            after_df.withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        latest_predictions.createOrReplaceTempView("stroke_predictions")
        print(f"✅ Processed {latest_predictions.count()} unique predictions")

        def write_to_postgres(df, table_name):
            """Write DataFrame to PostgreSQL cache table"""
            try:
                df_with_ts = df.withColumn("last_updated", F.current_timestamp())
                df_with_ts.write \
                    .mode("overwrite") \
                    .jdbc(url=postgres_url, table=table_name, properties=postgres_properties)
                print(f"✅ Written {df.count()} rows to {table_name}")
            except Exception as e:
                print(f"❌ Error writing to {table_name}: {str(e)}")
                raise

        # 1. Operation counts
        print("Calculating operation counts...")
        op_counts = df.groupBy("op").count()
        write_to_postgres(op_counts, "stroke_analytics_op_counts")

        # 2. Risk distribution
        print("Calculating risk distribution...")
        risk_dist = spark.sql("""
            SELECT risk_category, 
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM stroke_predictions
            GROUP BY risk_category
        """)
        write_to_postgres(risk_dist, "stroke_analytics_risk_distribution")

        # 3. Probability bins
        print("Calculating probability bins...")
        prob_bins = spark.sql("""
            SELECT 
                CASE 
                    WHEN probability < 0.2 THEN '0.0 - 0.2'
                    WHEN probability < 0.4 THEN '0.2 - 0.4'
                    WHEN probability < 0.6 THEN '0.4 - 0.6'
                    WHEN probability < 0.8 THEN '0.6 - 0.8'
                    ELSE '0.8 - 1.0'
                END AS prob_bucket,
                COUNT(*) AS total
            FROM stroke_predictions
            GROUP BY 1
        """)
        write_to_postgres(prob_bins, "stroke_analytics_probability_bins")

        # 4. Risk by gender
        print("Calculating risk by gender...")
        risk_gender = spark.sql("""
            SELECT 
                gender,
                SUM(CASE WHEN risk_category = 'High' THEN 1 ELSE 0 END) AS high_risk,
                SUM(CASE WHEN risk_category = 'Medium' THEN 1 ELSE 0 END) AS medium_risk,
                SUM(CASE WHEN risk_category = 'Low' THEN 1 ELSE 0 END) AS low_risk
            FROM stroke_predictions
            WHERE gender != 'Other'
            GROUP BY gender
        """)
        write_to_postgres(risk_gender, "stroke_analytics_risk_by_gender")

        # 5. Risk by age group
        print("Calculating risk by age group...")
        risk_age = spark.sql("""
            SELECT 
                CASE 
                    WHEN age < 30 THEN 'Under 30'
                    WHEN age BETWEEN 30 AND 45 THEN '30-45'
                    WHEN age BETWEEN 46 AND 60 THEN '46-60'
                    ELSE '60+'
                END AS age_group,
                risk_category,
                COUNT(*) AS total
            FROM stroke_predictions
            GROUP BY 1, 2
        """)
        write_to_postgres(risk_age, "stroke_analytics_risk_by_age_group")

        # 6. Province hotspots
        print("Calculating province hotspots...")
        province_hotspots = spark.sql("""
            SELECT 
                province,
                SUM(CASE WHEN risk_category = 'High' THEN 1 ELSE 0 END) AS high_risk,
                SUM(CASE WHEN risk_category = 'Medium' THEN 1 ELSE 0 END) AS medium_risk,
                SUM(CASE WHEN risk_category = 'Low' THEN 1 ELSE 0 END) AS low_risk
            FROM stroke_predictions
            WHERE country = 'South Africa'
            GROUP BY province
        """)
        write_to_postgres(province_hotspots, "stroke_analytics_province_hotspots")

        print("🎉 All analytics cached to PostgreSQL successfully!")
        
    except Exception as e:
        print(f"❌ Error in Spark analytics: {str(e)}")
        raise e
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()