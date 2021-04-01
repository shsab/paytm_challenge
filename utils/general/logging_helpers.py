import logging
import os
import pathlib
from logging import handlers

from utils.aws.aws_logging_handlers.S3 import S3Handler


def configure_logger(logger, env_vars, log_file_name, bucket=None, key=None):
    """
    Configure the logger

    :param logger:
    :param env_vars:
    :param log_file_name:
    :return:
    """
    current_files_path = pathlib.Path(__file__).parent.absolute()
    log_folder_path = os.path.join(current_files_path, '../../logs')
    pathlib.Path(log_folder_path).mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        '%(asctime)s::%(processName)s::%(name)s::%(levelname)s::%(funcName)s::%(message)s'
    )

    file_handler = handlers.TimedRotatingFileHandler(os.path.join(
        log_folder_path, f"{log_file_name}.log"),
        when='D',
        backupCount=7)
    stream_handler = logging.StreamHandler()
    cluster = ''
    if env_vars['CLUSTER_ID'] is not None:
        cluster = env_vars['CLUSTER_ID']

    if env_vars['LOG_TO_S3']:
        s3_handler = S3Handler(key=f"{key}{log_file_name}_{cluster}", bucket=bucket, workers=3)
        s3_handler.setFormatter(formatter)
        logger.addHandler(s3_handler)

    if env_vars['LOG_TO_FILE']:
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.INFO)