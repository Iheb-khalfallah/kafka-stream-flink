from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.udf import udf

# Define the Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
t_env = StreamTableEnvironment.create(env)
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)



# Kafka source table DDL for the new dataset
create_kafka_source_ddl = """
    CREATE TABLE patient_data (
        date STRING,
        time STRING,
        Patient_ID STRING,
        data ROW<hypertension DOUBLE, heart_disease DOUBLE, ever_married DOUBLE,
                 work_type DOUBLE, Residence_type DOUBLE, avg_glucose_level DOUBLE,
                 bmi DOUBLE, smoking_status DOUBLE>
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'urgent_data',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'test_3',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
"""

# Process the data
result_table = t_env.from_path("patient_data") \
    .select(
        col("Patient_ID"),  # Keep the Patient_ID in the result
        col("data.*")  # Include all fields in the 'data' nested column
    )

# Register the result table as an output table
t_env.create_temporary_view("result_view", result_table)

# Execute the job
t_env.execute_sql(create_kafka_source_ddl)
env.execute("PatientDataProcessing")

