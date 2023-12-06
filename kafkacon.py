import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # Create streaming environment
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

    # create table environment
    table_env = TableEnvironment.create(env_settings)

    #######################################################################
    # Create Kafka Source Tables with DDL for "data_urgent" and "data_normal"
    #######################################################################
    urgent_topic = 'urgent_data'
    normal_topic = 'normal_data'

    src_urgent_ddl = f"""
        CREATE TABLE e_health_urgent (
            patient_id INT,
            sex INT,
            age DOUBLE,
            hypertension INT,
            heart_disease INT,
            ever_married INT,
            work_type INT,
            Residence_type INT,
            avg_glucose_level DOUBLE,
            bmi DOUBLE,
            smoking_status INT
        ) WITH (
            'connector' = 'kafka_broker',
            'topic' = '{urgent_topic}',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'e-health-urgent',
            'format' = 'json'
        )
    """
    table_env.execute_sql(src_urgent_ddl)

    src_normal_ddl = f"""
        CREATE TABLE e_health_normal (
            patient_id INT,
            sex INT,
            age DOUBLE,
            hypertension INT,
            heart_disease INT,
            ever_married INT,
            work_type INT,
            Residence_type INT,
            avg_glucose_level DOUBLE,
            bmi DOUBLE,
            smoking_status INT
        ) WITH (
            'connector' = 'kafka_broker',
            'topic' = '{normal_topic}',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'e-health-normal',
            'format' = 'json'
        )
    """
    table_env.execute_sql(src_normal_ddl)

    # Create and initiate loading of source Tables
    tbl_urgent = table_env.from_path('e_health_urgent')
    tbl_normal = table_env.from_path('e_health_normal')

    print('\nSource Schema (e_health_urgent)')
    tbl_urgent.print_schema()

    print('\nSource Schema (e_health_normal)')
    tbl_normal.print_schema()



    ###############################################################
    # Create Kafka Sink Tables for "e-health-result-urgent" and "e-health-result-normal"
    ###############################################################
    sink_urgent_ddl = """
        CREATE TABLE e_health_result_urgent (
            sex INT,
            window_end TIMESTAMP(3),
            avg_age DOUBLE
        ) WITH (
            'connector' = 'kafka_broker',
            'topic' = 'urgent_data',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    table_env.execute_sql(sink_urgent_ddl)
    
    result_table_schema = table_env.from_path('e_health_result_urgent').get_schema()
    print('\nSink Schema (e_health_result_urgent)')
    print(result_table_schema)

    sink_normal_ddl = """
        CREATE TABLE e_health_result_normal (
            sex INT,
            window_end TIMESTAMP(3),
            avg_age DOUBLE
        ) WITH (
            'connector' = 'kafka_broker',
            'topic' = 'normal_data',
            'properties.bootstrap.servers' = 'localhost:9092',
            'format' = 'json'
        )
    """
    table_env.execute_sql(sink_normal_ddl)
    
    result_table_schema = table_env.from_path('e_health_result_normal').get_schema()
    print('\nSink Schema (e_health_result_normal)')
    print(result_table_schema)


if __name__ == '__main__':
    main()
