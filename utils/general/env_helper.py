import configparser
import os
import pathlib

from dotenv import load_dotenv, find_dotenv


def set_env():
    # Load environment file
    load_dotenv(find_dotenv())

    cur_env = dict()
    cur_env['CLUSTER_ID'] = os.getenv('EMR_CLUSTER_ID', None)
    cur_env['STEP_ID'] = os.getenv('EMR_STEP_ID', None)
    # Control the flow
    cur_env['LOG_TO_FILE'] = os.getenv('LOG_TO_FILE', 'True') == 'True'
    cur_env['LOG_TO_S3'] = os.getenv('LOG_TO_S3', 'True') == 'True'
    cur_env['SPARK_USE_EMR'] = os.getenv('SPARK_USE_EMR', 'False') == 'True'
    cur_env['INSERT_DB_FRAME_SIZE'] = int(os.getenv('INSERT_DB_FRAME_SIZE', 100000))
    cur_env['SPARK_DEF_PARTITIONS'] = int(os.getenv('SPARK_DEF_PARTITIONS', 5))

    # Setup Sparks access to s3
    S3_ACCESS_KEY_ID = None
    S3_ACCESS_KEY_SECRET = None
    try:
        path = pathlib.PosixPath('~/.aws/credentials')
        config = configparser.RawConfigParser()
        config.read(path.expanduser())
        S3_ACCESS_KEY_ID = config.get('default', 'aws_access_key_id')
        S3_ACCESS_KEY_SECRET = config.get('default', 'aws_secret_access_key')
    except Exception as err:
        pass

    # if access key and secret provided as env variable it will use them
    cur_env['S3_ACCESS_KEY_ID'] = os.getenv('S3_ACCESS_KEY_ID', S3_ACCESS_KEY_ID)
    cur_env['S3_ACCESS_KEY_SECRET'] = os.getenv('S3_ACCESS_KEY_SECRET', S3_ACCESS_KEY_SECRET)

    # Create Spark Session
    cur_env['HADOOP_AWS_VERSION'] = os.getenv('HADOOP_AWS_VERSION', '2.9.2')
    cur_env['SPARK_MASTER'] = os.getenv('SPARK_MASTER', 'local[*]')
    return cur_env