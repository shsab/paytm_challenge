"""
You may need to set up some environment variables in this way to reduce hard coding
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export S3_BUCKET_NAME=<the_bucket_name>. e.g. fh-data-science
export S3_KEY_PREFIX=<the_path_under_S3_BUCKET_NAME>. e.g. risk-analysis/McDonald-pos-data
export DATA_DIR=<your_local_dir_if_you_need_to_download_s3_files>
"""
import logging
from functools import partial
from pathlib import Path
from typing import List
import os
import re
import datetime
import boto3
from boto3.s3.transfer import S3Transfer
from tqdm import tqdm

logger = logging.getLogger()


def download_s3_files(s3_bucket: str,
                      s3_key_prefix: str,
                      local_path: str,
                      pattern: str = None,
                      return_latest: bool = False,
                      show_progressbar: bool = True,
                      clean_local: bool = False
                      ):
    """
    Download S3 files to local path recursively. These params should be put into some env variables especially 'local_path'
    :param s3_bucket: s3 bucket name. e.g. fh-data-science
    :param s3_key_prefix: s3 path under bucket name: e.g. risk-analysis/McDonald-pos-data (keep this format and don't put
                        '/' at the beginning and ending)
    :param local_path: Your local path to store S3 files
    :return:
    """
    s3_client = boto3.client('s3')
    transfer = S3Transfer(s3_client)
    resp = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_key_prefix)

    file_contents = list(filter(lambda x: x['Size'] > 0, resp['Contents']))

    # filters the file list if pattern is provided otherwise download the whole list
    if pattern is not None:
        file_contents = [f for f in file_contents if pattern in f['Key']]

    # Sort based on last modified DESC
    file_contents = sorted(file_contents,
                           key=lambda x: x['LastModified'],
                           reverse=True)

    if return_latest and len(file_contents) > 0:
        file_contents = file_contents[0]

    if clean_local:
        logger.info(f'Cleaning the local path. {local_path}')
        for f in Path(local_path).glob(f"*{pattern}"):
            try:
                f.unlink()
            except OSError as e:
                logger.error("Error: %s : %s" % (f, e.strerror))
                pass

    local_file_paths = []
    for file_content in file_contents:
        s3_path = file_content['Key'].replace(s3_key_prefix, '')
        local_file_path = local_path + s3_path

        folder = local_file_path[:local_file_path.rfind('/')]
        Path(folder).mkdir(parents=True, exist_ok=True)

        logger.info('Downloading from S3: ' + file_content['Key'])

        if show_progressbar:
            def hook(t):
                def inner(bytes_amount):
                    t.update(bytes_amount)
                return inner

            with tqdm(total=file_content['Size'],
                      unit='B',
                      unit_scale=True,
                      unit_divisor=1024,
                      miniters=1,
                      desc=file_content['Key']) as t:  # all optional kwargs
                transfer.download_file(bucket=s3_bucket,
                                       key=file_content['Key'],
                                       filename=local_file_path,
                                       callback=hook(t))
        else:
            transfer.download_file(bucket=s3_bucket,
                                   key=file_content['Key'],
                                   filename=local_file_path)
        local_file_paths.append(local_file_path)

        logger.info('Downloaded from S3: ' + file_content['Key'])
    return local_file_paths


def download_latest_files_from_s3(s3_bucket_name,
                                  s3_key_prefix,
                                  local_path,
                                  file_extension: str = 'xlsx') -> List[str]:
    """ For each sub-directory in the specified S3 bucket, downloads the
    most recent file. Only goes one sub-directory deep.

    Old behaviour was constructing the file name assuming the file extension is passed in my use case
    I want to filter by file name, not file extension I reused the file_extension parameter and passed
    the file_name that I am looking for and introduced the local_file_name with default value None.
    If local_file_name is provided then it bypass the file name construction part and use local_file_name.

    :param s3_bucket_name: Name of the S3 bucket
    :dtype s3_bucket_name: String

    :param s3_key_prefix: Sub-directory within bucket.
    :dtype s3_key_prefix: String

    :param local_path: Path to save local files
    :dtype local_path: String

    :param file_extension: File extension pattern to search for w/out
                            the period. E.g. csv, xlsx, txt
    :dtype file_extension: String

    :return: List of newly saved files
    :rtype: List[str]
    """
    s3 = boto3.client('s3')
    Path(local_path).mkdir(parents=True, exist_ok=True)

    # List the objects in the S3 bucket
    res = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_key_prefix)

    if s3_key_prefix[-1] != '/':
        s3_key_prefix = s3_key_prefix + '/'

    # FInd all the sub directories. Filter out the s3_key_prefix to avoid
    # having doubles of the newest file.
    key_directory_prefix_list = list(
        filter(
            lambda x: re.search(r'\D/$', x['Key']) is not None and x['Key'] !=
            s3_key_prefix, res['Contents']))

    local_file_paths = []
    for key_directory_prefix in key_directory_prefix_list:
        # Find the contents of the sub-directory
        contents = s3.list_objects_v2(
            Bucket=s3_bucket_name,
            Prefix=key_directory_prefix['Key'])['Contents']

        # Find all the files that end with the specified file extension.
        file_contents = list(
            filter(lambda x: x['Key'].endswith(file_extension), contents))

        latest_file_content = sorted(file_contents,
                                     key=lambda x: x['LastModified'],
                                     reverse=True)

        if len(latest_file_content) > 0:
            latest_file = latest_file_content[0]['Key']

            # Rename the file with the datetime stamp of download.
            file_name: str = latest_file.split('/')[-1]

            local_file_name = file_name[:file_name.index(f'.{file_extension}')] + \
                              '-' + str(datetime.datetime.now().timestamp()) + \
                              file_name[file_name.index(f'.{file_extension}'):]

            # Download the file.
            local_file_path = os.path.join(local_path, local_file_name)
            s3.download_file(Bucket=s3_bucket_name,
                             Key=latest_file,
                             Filename=local_file_path)
            local_file_paths.append(local_file_path)

    return local_file_paths


def list_all_files_s3(s3_bucket_name,
                      s3_key_prefix,
                      pattern):
    """
    Gets all the files in the bucket under the key (folder) provided that matches the pattern.

    :param s3_bucket_name:
    :param s3_key_prefix:
    :param pattern:
    :return:
    """
    s3 = boto3.client('s3')
    # List the objects in the S3 bucket
    res = s3.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_key_prefix)

    if s3_key_prefix[-1] != '/':
        s3_key_prefix = s3_key_prefix + '/'

    # FInd all the sub directories. Filter out the s3_key_prefix to avoid
    # having doubles of the newest file.
    files_list = [
        x['Key'] for x in filter(lambda x: re.search(r'\D/$', x['Key']) is None and x['Key'].startswith(s3_key_prefix) and pattern in x['Key'], res['Contents'])
    ]
    return files_list


