#!/usr/bin/env python3
"""
Enhanced Real-Time Credit Card Fraud Detection Pipeline
Features:
- Machine Learning-based fraud detection
- Delta Lake for ACID transactions
- Comprehensive logging and monitoring
- Real-time alerting
- Data quality validation
- Advanced feature engineering
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, when, lit, udf, expr, 
    current_timestamp, year, month, dayofmonth, hour, 
    dayofweek, quarter, weekofyear, date_format,
    sha2, concat, row_number, lag, avg, stddev, count,
    sum as spark_sum, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StringType, StructField, DecimalType, 
    TimestampType, DoubleType, IntegerType, BooleanType
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FraudDetectionConfig:
    """Configuration class for fraud detection parameters"""
    
    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = "pkc-p11xm.us-east-1.aws.confluent.cloud:9092"
        self.kafka_topic = "new"
        self.kafka_username = "NQUVTTMSN2ZERMOC"
        self.kafka_password = "H5D3zLCvpITwlG+erBnSp1uVtQkjYeZpFqoQ38RdO0lHymht4ny68cCZ7KT++S36"
        
        # AWS Configuration
        self.s3_historical_data = "s3://credit-project-data-80-percent/part-00000-5c9dab29-f21f-4f32-821a-4c9d76f329e8-c000.csv"
        self.s3_processed_data = "s3://athenabucket-231/delta_table/"
        self.s3_model_path = "s3://athenabucket-231/models/"
        
        # RDS Configuration
        self.rds_url = "jdbc:mysql://grouptwo.cq7tmru9zell.us-east-1.rds.amazonaws.com/customer"
        self.rds_user = "admin"
        self.rds_password = "Group_two"
        
        # Fraud Detection Parameters
        self.distance_threshold = 1500  # km
        self.amount_threshold = 500     # USD
        self.ml_model_enabled = True
        self.anomaly_detection_enabled = True
        
        # Performance Parameters
        self.checkpoint_location = "/tmp/checkpoint"
        self.trigger_interval = "5 seconds"
        self.max_offsets_per_trigger = 10000

class DistanceCalculator:
    """Utility class for geographic calculations"""
    
    @staticmethod
    def calculate_distance(lat1, lon1, lat2, lon2):
        """Calculate Haversine distance between two points"""
        R = 6371.0  # Earth radius in kilometers
        
        lat1_rad = expr("radians(lat1)")
        lon1_rad = expr("radians(lon1)")
        lat2_rad = expr("radians(lat2)")
        lon2_rad = expr("radians(lon2)")
        
        d_lat = lat2_rad - lat1_rad
        d_lon = lon2_rad - lon1_rad
        
        a = expr("sin(d_lat/2)^2 + cos(lat1_rad) * cos(lat2_rad) * sin(d_lon/2)^2")
        c = expr("2 * atan2(sqrt(a), sqrt(1-a))")
        
        return R * c

class FeatureEngineer:
    """Class for advanced feature engineering"""
    
    @staticmethod
    def add_temporal_features(df: DataFrame) -> DataFrame:
        """Add time-based features for fraud detection"""
        return df.withColumn("timestamp", current_timestamp()) \
                 .withColumn("year", year("timestamp")) \
                 .withColumn("month", month("timestamp")) \
                 .withColumn("day", dayofmonth("timestamp")) \
                 .withColumn("hour", hour("timestamp")) \
                 .withColumn("day_of_week", dayofweek("timestamp")) \
                 .withColumn("quarter", quarter("timestamp")) \
                 .withColumn("week_of_year", weekofyear("timestamp")) \
                 .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), True).otherwise(False)) \
                 .withColumn("is_business_hours", when(
                     (col("hour") >= 9) & (col("hour") <= 17), True
                 ).otherwise(False))
    
    @staticmethod
    def add_transaction_features(df: DataFrame) -> DataFrame:
        """Add transaction-specific features"""
        return df.withColumn("amount_log", expr("log(amt + 1)")) \
                 .withColumn("amount_squared", expr("amt * amt")) \
                 .withColumn("is_high_value", when(col("amt") > 1000, True).otherwise(False)) \
                 .withColumn("is_low_value", when(col("amt") < 10, True).otherwise(False))
    
    @staticmethod
    def add_customer_behavior_features(df: DataFrame, historical_df: DataFrame) -> DataFrame:
        """Add customer behavior patterns"""
        # Calculate customer statistics
        customer_stats = historical_df.groupBy("cc_num").agg(
            avg("amt").alias("avg_amount"),
            stddev("amt").alias("std_amount"),
            count("*").alias("total_transactions"),
            spark_max("amt").alias("max_amount"),
            spark_min("amt").alias("min_amount"),
            avg("distance").alias("avg_distance")
        )
        
        # Join with current transactions
        return df.join(customer_stats, "cc_num", "left") \
                 .withColumn("amount_deviation", 
                           (col("amt") - col("avg_amount")) / col("std_amount")) \
                 .withColumn("is_unusual_amount", 
                           when(abs(col("amount_deviation")) > 2, True).otherwise(False))

class MLFraudDetector:
    """Machine Learning-based fraud detection"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model = None
        self.scaler = None
        self.feature_columns = [
            "amount_log", "distance", "hour", "day_of_week", 
            "is_weekend", "is_business_hours", "amount_deviation"
        ]
    
    def prepare_features(self, df: DataFrame) -> DataFrame:
        """Prepare features for ML model"""
        # Ensure all feature columns exist
        for col_name in self.feature_columns:
            if col_name not in df.columns:
                df = df.withColumn(col_name, lit(0.0))
        
        # Fill null values
        for col_name in self.feature_columns:
            df = df.fillna(0.0, subset=[col_name])
        
        return df
    
    def train_model(self, training_data: DataFrame):
        """Train the fraud detection model"""
        logger.info("Training fraud detection model...")
        
        # Prepare features
        feature_cols = [col(c) for c in self.feature_columns]
        assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Train Random Forest classifier
        rf = RandomForestClassifier(
            labelCol="is_fraud",
            featuresCol="scaled_features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Train model
        self.model = pipeline.fit(training_data)
        self.scaler = scaler
        
        logger.info("Model training completed")
        
        # Evaluate model
        predictions = self.model.transform(training_data)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="is_fraud", 
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        logger.info(f"Model accuracy: {accuracy:.4f}")
    
    def predict_fraud(self, df: DataFrame) -> DataFrame:
        """Predict fraud using trained model"""
        if self.model is None:
            logger.warning("Model not trained, using rule-based detection")
            return self._rule_based_detection(df)
        
        # Prepare features
        df_features = self.prepare_features(df)
        
        # Make predictions
        predictions = self.model.transform(df_features)
        
        return predictions.withColumn(
            "ml_fraud_score", 
            when(col("prediction") == 1, col("probability")[1]).otherwise(col("probability")[0])
        )
    
    def _rule_based_detection(self, df: DataFrame) -> DataFrame:
        """Fallback rule-based detection"""
        return df.withColumn(
            "rule_based_fraud",
            when(
                (col("distance") > 1500) & (col("amt") > 1000),
                "HIGH_RISK"
            ).when(
                (col("distance") > 1000) & (col("amt") > 500),
                "MEDIUM_RISK"
            ).otherwise("LOW_RISK")
        )

class AnomalyDetector:
    """Anomaly detection using clustering"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.kmeans_model = None
    
    def train_clustering_model(self, training_data: DataFrame):
        """Train K-means clustering model for anomaly detection"""
        logger.info("Training anomaly detection model...")
        
        # Prepare features for clustering
        feature_cols = ["amount_log", "distance", "hour"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="clustering_features")
        
        # Scale features
        scaler = StandardScaler(inputCol="clustering_features", outputCol="scaled_clustering_features")
        
        # Train K-means
        kmeans = KMeans(k=3, seed=42, featuresCol="scaled_clustering_features")
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train model
        self.kmeans_model = pipeline.fit(training_data)
        logger.info("Anomaly detection model training completed")
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect anomalies using clustering"""
        if self.kmeans_model is None:
            logger.warning("Anomaly detection model not trained")
            return df.withColumn("anomaly_score", lit(0.0))
        
        # Make predictions
        predictions = self.kmeans_model.transform(df)
        
        # Calculate anomaly score based on distance to cluster center
        return predictions.withColumn(
            "anomaly_score",
            when(col("prediction") == 0, 0.1)
            .when(col("prediction") == 1, 0.5)
            .otherwise(0.9)
        )

class DataQualityValidator:
    """Data quality validation and monitoring"""
    
    @staticmethod
    def validate_schema(df: DataFrame, expected_schema: StructType) -> bool:
        """Validate DataFrame schema"""
        try:
            actual_schema = df.schema
            if actual_schema != expected_schema:
                logger.warning(f"Schema mismatch: expected {expected_schema}, got {actual_schema}")
                return False
            return True
        except Exception as e:
            logger.error(f"Schema validation error: {e}")
            return False
    
    @staticmethod
    def check_data_quality(df: DataFrame) -> Dict[str, Any]:
        """Perform comprehensive data quality checks"""
        quality_metrics = {}
        
        try:
            # Basic counts
            quality_metrics["total_records"] = df.count()
            quality_metrics["null_ssn_count"] = df.filter(col("ssn").isNull()).count()
            quality_metrics["null_cc_num_count"] = df.filter(col("cc_num").isNull()).count()
            quality_metrics["null_amount_count"] = df.filter(col("amt").isNull()).count()
            
            # Data validation
            quality_metrics["invalid_amount_count"] = df.filter(col("amt") <= 0).count()
            quality_metrics["invalid_coordinates_count"] = df.filter(
                (col("lat").isNull()) | (col("long").isNull())
            ).count()
            
            # Duplicate detection
            quality_metrics["duplicate_transactions"] = df.groupBy("trans_num").count() \
                .filter(col("count") > 1).count()
            
            # Calculate quality scores
            total_records = quality_metrics["total_records"]
            if total_records > 0:
                quality_metrics["data_quality_score"] = (
                    (total_records - sum([
                        quality_metrics["null_ssn_count"],
                        quality_metrics["null_cc_num_count"],
                        quality_metrics["null_amount_count"],
                        quality_metrics["invalid_amount_count"],
                        quality_metrics["duplicate_transactions"]
                    ])) / total_records
                ) * 100
            else:
                quality_metrics["data_quality_score"] = 0
            
            logger.info(f"Data quality score: {quality_metrics['data_quality_score']:.2f}%")
            
        except Exception as e:
            logger.error(f"Data quality check error: {e}")
            quality_metrics["error"] = str(e)
        
        return quality_metrics

class FraudAlertManager:
    """Manages fraud alerts and notifications"""
    
    def __init__(self, spark: SparkSession, sns_topic_arn: str = None):
        self.spark = spark
        self.sns_topic_arn = sns_topic_arn
        self.alert_thresholds = {
            "HIGH_RISK": 0.8,
            "MEDIUM_RISK": 0.6,
            "LOW_RISK": 0.4
        }
    
    def generate_alerts(self, df: DataFrame) -> DataFrame:
        """Generate fraud alerts based on risk scores"""
        return df.withColumn(
            "risk_level",
            when(col("ml_fraud_score") >= self.alert_thresholds["HIGH_RISK"], "HIGH_RISK")
            .when(col("ml_fraud_score") >= self.alert_thresholds["MEDIUM_RISK"], "MEDIUM_RISK")
            .when(col("ml_fraud_score") >= self.alert_thresholds["LOW_RISK"], "LOW_RISK")
            .otherwise("NO_RISK")
        ).withColumn(
            "alert_generated",
            when(col("risk_level").isin("HIGH_RISK", "MEDIUM_RISK"), True).otherwise(False)
        )
    
    def send_alert_notification(self, alert_data: Dict[str, Any]):
        """Send alert notification (placeholder for SNS integration)"""
        if alert_data.get("risk_level") in ["HIGH_RISK", "MEDIUM_RISK"]:
            logger.warning(f"FRAUD ALERT: {alert_data}")
            # TODO: Integrate with AWS SNS for real notifications

class EnhancedFraudDetectionPipeline:
    """Main fraud detection pipeline class"""
    
    def __init__(self, config: FraudDetectionConfig):
        self.config = config
        self.spark = None
        self.ml_detector = None
        self.anomaly_detector = None
        self.feature_engineer = FeatureEngineer()
        self.quality_validator = DataQualityValidator()
        self.alert_manager = None
        
        # Schema definition
        self.transaction_schema = StructType([
            StructField("_c0", StringType(), True),
            StructField("ssn", StringType(), True),
            StructField("cc_num", StringType(), True),
            StructField("first", StringType(), True),
            StructField("last", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", DecimalType(5, 0), True),
            StructField("lat", DecimalType(9, 6), True),
            StructField("long", DecimalType(9, 6), True),
            StructField("city_pop", StringType(), True),
            StructField("job", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("acct_num", StringType(), True),
            StructField("profile", StringType(), True),
            StructField("trans_num", StringType(), True),
            StructField("trans_date", StringType(), True),
            StructField("trans_time", StringType(), True),
            StructField("unix_time", StringType(), True),
            StructField("category", StringType(), True),
            StructField("amt", DecimalType(9, 2), True),
            StructField("is_fraud", DecimalType(9, 0), True),
            StructField("merchant", StringType(), True),
            StructField("merch_lat", DecimalType(9, 6), True),
            StructField("merch_long", DecimalType(9, 6), True),
        ])
    
    def initialize_spark(self):
        """Initialize Spark session with optimized configuration"""
        logger.info("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName("Enhanced_Fraud_Detection_Pipeline") \
            .config("spark.jars", "s3://requiredfiles2323/spark-sql-kafka-0-10_2.12:3.2.1.jar") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", self.config.checkpoint_location) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized successfully")
    
    def load_historical_data(self) -> DataFrame:
        """Load and cache historical transaction data"""
        logger.info("Loading historical transaction data...")
        
        try:
            historical_df = self.spark.read.csv(
                self.config.s3_historical_data, 
                header=True, 
                schema=self.transaction_schema
            ).cache()
            
            logger.info(f"Loaded {historical_df.count()} historical transactions")
            return historical_df
            
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            raise
    
    def setup_kafka_stream(self) -> DataFrame:
        """Setup Kafka streaming source"""
        logger.info("Setting up Kafka stream...")
        
        try:
            kafka_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers) \
                .option("subscribe", self.config.kafka_topic) \
                .option("kafka.security.protocol", "SASL_SSL") \
                .option("kafka.sasl.mechanism", "PLAIN") \
                .option("kafka.sasl.jaas.config", 
                        f"org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        f"username='{self.config.kafka_username}' " +
                        f"password='{self.config.kafka_password}';") \
                .option("maxOffsetsPerTrigger", self.config.max_offsets_per_trigger) \
                .load()
            
            # Parse Kafka messages
            parsed_stream = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                .select(from_json(col("value"), self.transaction_schema).alias("data")) \
                .select("data.*")
            
            logger.info("Kafka stream setup completed")
            return parsed_stream
            
        except Exception as e:
            logger.error(f"Error setting up Kafka stream: {e}")
            raise
    
    def setup_rds_connection(self) -> DataFrame:
        """Setup RDS connection for customer data"""
        logger.info("Setting up RDS connection...")
        
        try:
            connection_properties = {
                "user": self.config.rds_user,
                "password": self.config.rds_password,
                "driver": "com.mysql.jdbc.Driver"
            }
            
            rds_df = self.spark.read.jdbc(
                self.config.rds_url, 
                table="customer_details", 
                properties=connection_properties
            )
            
            logger.info("RDS connection established")
            return rds_df
            
        except Exception as e:
            logger.error(f"Error connecting to RDS: {e}")
            raise
    
    def initialize_ml_models(self, historical_df: DataFrame):
        """Initialize and train ML models"""
        if not self.config.ml_model_enabled:
            logger.info("ML models disabled, skipping initialization")
            return
        
        logger.info("Initializing ML models...")
        
        try:
            # Initialize ML components
            self.ml_detector = MLFraudDetector(self.spark)
            self.anomaly_detector = AnomalyDetector(self.spark)
            
            # Prepare training data
            training_data = historical_df.filter(col("is_fraud").isNotNull())
            
            if training_data.count() > 0:
                # Train fraud detection model
                self.ml_detector.train_model(training_data)
                
                # Train anomaly detection model
                self.anomaly_detector.train_clustering_model(training_data)
                
                logger.info("ML models initialized and trained successfully")
            else:
                logger.warning("No training data available, ML models not trained")
                
        except Exception as e:
            logger.error(f"Error initializing ML models: {e}")
            logger.info("Falling back to rule-based detection")
    
    def process_streaming_data(self, streaming_df: DataFrame, historical_df: DataFrame) -> DataFrame:
        """Process streaming data with enhanced features"""
        logger.info("Processing streaming data...")
        
        try:
            # Add temporal features
            df_with_time = self.feature_engineer.add_temporal_features(streaming_df)
            
            # Add transaction features
            df_with_transaction = self.feature_engineer.add_transaction_features(df_with_time)
            
            # Add customer behavior features
            df_with_behavior = self.feature_engineer.add_customer_behavior_features(
                df_with_transaction, historical_df
            )
            
            # Calculate distance
            df_with_distance = df_with_behavior.withColumn(
                "distance", 
                DistanceCalculator.calculate_distance(
                    col("lat"), col("long"), col("merch_lat"), col("merch_long")
                )
            )
            
            # ML-based fraud detection
            if self.ml_detector and self.ml_detector.model is not None:
                df_with_ml = self.ml_detector.predict_fraud(df_with_distance)
            else:
                df_with_ml = df_with_distance.withColumn("ml_fraud_score", lit(0.5))
            
            # Anomaly detection
            if self.anomaly_detector and self.anomaly_detector.kmeans_model is not None:
                df_with_anomaly = self.anomaly_detector.detect_anomalies(df_with_ml)
            else:
                df_with_anomaly = df_with_ml.withColumn("anomaly_score", lit(0.5))
            
            # Generate alerts
            if self.alert_manager:
                df_with_alerts = self.alert_manager.generate_alerts(df_with_anomaly)
            else:
                df_with_alerts = df_with_anomaly.withColumn("risk_level", lit("UNKNOWN"))
            
            # Final fraud status
            final_df = df_with_alerts.withColumn(
                "final_fraud_status",
                when(
                    (col("ml_fraud_score") > 0.7) | 
                    (col("anomaly_score") > 0.8) |
                    ((col("distance") > self.config.distance_threshold) & 
                     (col("amt") > self.config.amount_threshold)),
                    "FRAUD"
                ).otherwise("GENUINE")
            )
            
            logger.info("Streaming data processing completed")
            return final_df
            
        except Exception as e:
            logger.error(f"Error processing streaming data: {e}")
            raise
    
    def write_to_delta_lake(self, df: DataFrame, epoch_id: int):
        """Write processed data to Delta Lake"""
        try:
            if not df.isEmpty():
                # Add processing metadata
                df_with_metadata = df.withColumn("processing_timestamp", current_timestamp()) \
                                    .withColumn("epoch_id", lit(epoch_id))
                
                # Write to Delta Lake with partitioning
                df_with_metadata.write \
                    .format("delta") \
                    .mode("append") \
                    .partitionBy("year", "month", "final_fraud_status") \
                    .save(self.config.s3_processed_data)
                
                logger.info(f"Data written to Delta Lake for epoch {epoch_id}")
                
        except Exception as e:
            logger.error(f"Error writing to Delta Lake: {e}")
            raise
    
    def write_to_rds(self, df: DataFrame, epoch_id: int):
        """Write customer data to RDS"""
        try:
            if not df.isEmpty():
                # Select customer fields for RDS
                customer_df = df.select(
                    "ssn", "cc_num", "first", "last", "gender", 
                    "city", "state", "dob", "acct_num", "final_fraud_status"
                )
                
                # Write to RDS
                connection_properties = {
                    "user": self.config.rds_user,
                    "password": self.config.rds_password,
                    "driver": "com.mysql.jdbc.Driver"
                }
                
                customer_df.write.jdbc(
                    self.config.rds_url, 
                    table="customer_details", 
                    mode="append", 
                    properties=connection_properties
                )
                
                logger.info(f"Customer data written to RDS for epoch {epoch_id}")
                
        except Exception as e:
            logger.error(f"Error writing to RDS: {e}")
            raise
    
    def run_pipeline(self):
        """Main pipeline execution"""
        try:
            logger.info("Starting Enhanced Fraud Detection Pipeline...")
            
            # Initialize components
            self.initialize_spark()
            historical_df = self.load_historical_data()
            streaming_df = self.setup_kafka_stream()
            rds_df = self.setup_rds_connection()
            
            # Initialize ML models
            self.initialize_ml_models(historical_df)
            
            # Setup alert manager
            self.alert_manager = FraudAlertManager(self.spark)
            
            # Process streaming data
            processed_df = self.process_streaming_data(streaming_df, historical_df)
            
            # Start streaming queries
            query1 = processed_df.writeStream \
                .foreachBatch(self.write_to_delta_lake) \
                .outputMode("append") \
                .trigger(processingTime=self.config.trigger_interval) \
                .start()
            
            query2 = processed_df.writeStream \
                .foreachBatch(self.write_to_rds) \
                .outputMode("append") \
                .trigger(processingTime=self.config.trigger_interval) \
                .start()
            
            logger.info("Pipeline started successfully")
            
            # Wait for termination
            query1.awaitTermination()
            query2.awaitTermination()
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

def main():
    """Main entry point"""
    try:
        # Initialize configuration
        config = FraudDetectionConfig()
        
        # Create and run pipeline
        pipeline = EnhancedFraudDetectionPipeline(config)
        pipeline.run_pipeline()
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    main()
