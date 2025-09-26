# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
# spark-submit --master spark://localhost:7077 spark_stream.py

import logging
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };
                    """)
    logger.info("Keyspace created successfully")

def create_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.users (
                        id UUID PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        gender TEXT,
                        address TEXT,  
                        email TEXT,
                        username TEXT,
                        dob TEXT,
                        registered_date TEXT,
                        phone TEXT,
                        picture TEXT
                    );
                    """)
    logger.info("Table created successfully")

# def insert_data(session, **kwargs):
#     logger.info("Inserting data into Cassandra")
#     id = kwargs['id']
#     first_name = kwargs['first_name']
#     last_name = kwargs['last_name']
#     gender = kwargs['gender']
#     address = kwargs['address']
#     email = kwargs['email']
#     username = kwargs['username']
#     registered_date = kwargs['registered_date']
#     phone = kwargs['phone']
#     picture = kwargs['picture']

#     try:
#         session.execute("""
#                         INSERT INTO spark_streams.users (id, first_name, last_name, gender, address, email, username, registered_date, phone, picture)
#                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#                         """, (id, first_name, last_name, gender, address, email, username, registered_date, phone, picture))
#         logger.info("Data inserted successfully")
#     except Exception as e:
#         logger.error(f"Error inserting data into Cassandra: {e}")

def connect_to_kafka(spark_conn):
    import os
    try:
        kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", "user_data") \
            .option("startingOffsets", "latest") \
            .load()
        logger.info(f"Connected to Kafka successfully at {kafka_bootstrap_servers}")
        return df
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        return None

def create_selection_df_from_kafka(df):
    try:
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("dob", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False)
        ])
        df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
        logger.info("Selection DataFrame created successfully")
        return df
    except Exception as e:
        logger.error(f"Error creating selection DataFrame from Kafka: {e}")
        return None

def create_spark_connection():
    import os
    try:
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        conn = SparkSession.builder \
            .appName('sparkDataStreaming') \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
            .config('spark.cassandra.connection.host', cassandra_host) \
            .getOrCreate()
        conn.sparkContext.setLogLevel('ERROR')
        logging.info(f"Spark connection created successfully with Cassandra host: {cassandra_host}")   
        return conn
    except Exception as e:
        logging.error(f"Error creating Spark connection: {e}")
        return None

def create_cassandra_connection():
    import os
    try:
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cluster = Cluster([cassandra_host])
        cass_session = cluster.connect()
        logging.info(f"Cassandra connection created successfully to {cassandra_host}")
        return cass_session
    except Exception as e:
        logging.error(f"Error creating Cassandra connection: {e}")
        return None

def write_streaming_data_to_cassandra(df):
    """Write streaming data to Cassandra using Spark connector"""
    try:
        logging.info("Starting streaming write to Cassandra")
        
        # This uses the spark-cassandra-connector JAR you loaded
        query = df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spark_streams") \
            .option("table", "users") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start()
            
        logging.info("Streaming query started successfully")
        return query
        
    except Exception as e:
        logging.error(f"Error writing streaming data to Cassandra: {e}")
        return None

def create_keyspace_and_table_with_spark(spark_session):
    """Create keyspace and table using Spark instead of manual Cassandra connection"""
    try:
        logging.info("Creating keyspace and table using Spark")
        
        # Create keyspace using Spark SQL
        spark_session.sql("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """)
        logging.info("Keyspace created successfully with Spark")
        
        # Create table using Spark SQL  
        spark_session.sql("""
            CREATE TABLE IF NOT EXISTS spark_streams.users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                email TEXT,
                username TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            )
        """)
        logging.info("Table created successfully with Spark")
        
    except Exception as e:
        logging.error(f"Error creating keyspace/table with Spark: {e}")


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)
            if selection_df is not None:
                # Setup schema using manual connection (more reliable)
                session = create_cassandra_connection()
                if session is not None:
                    create_keyspace(session)
                    create_table(session)
                    session.shutdown()  # Close the manual connection
                    
                    # Start streaming
                    streaming_query = write_streaming_data_to_cassandra(selection_df)
                    if streaming_query:
                        try:
                            streaming_query.awaitTermination()
                        except KeyboardInterrupt:
                            logging.info("Stopping streaming query...")
                            streaming_query.stop()
                    else:
                        logging.error("Failed to start streaming query")
                else:
                    logging.error("Failed to create Cassandra connection")
            else:
                logging.error("Failed to create selection DataFrame")
        else:
            logging.error("Failed to connect to Kafka")
    else:
        logging.error("Failed to create Spark connection")
