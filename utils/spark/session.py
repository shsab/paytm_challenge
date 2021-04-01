from pyspark import SparkConf
from pyspark.sql import SparkSession
from typing import Union


def set_spark(use_emr: bool = False,
              spark_master: str = None,
              set_s3: bool = True,
              hadoop_aws_version: str = None,
              aws_s3_access_key_id: str = None,
              aws_s3_access_key_secret: str = None,
              app_name: str = "FH-Saprk-App",
              additional_packages: Union[str, list] = None) -> SparkSession:
    """
    Set the Spark session


    :param use_emr: To control the config. If True refrain from setting some of the configs and leave it to EMR.
    :type use_emr: str

    :param spark_master: Not necessary if use_emr is True. You should pass the cluster master or Local[*].
    :type spark_master: str

    :param set_s3: Default is True. In case you don't want to set the s3 config you can set this to False. Only in effect
                   when use_emr is false.
    :type set_s3: bool

    :param hadoop_aws_version: The hadoop version that your spark is build for. Only necessary when set_s3 is set to True
                               and use_emr is false.
    :type hadoop_aws_version: str

    :param aws_s3_access_key_id: The s3 access ID. Only necessary when set_s3 is set to True
                                 and use_emr is false.
    :type aws_s3_access_key_id: str

    :param aws_s3_access_key_secret: The s3 access secret. Only necessary when set_s3 is set to True
                                     and use_emr is false.
    :type aws_s3_access_key_secret: str

    :param app_name: Application name
    :type app_name: str

    :return: spark session
    :rtype: pyspark.sql.SparkSession
    """

    spark_packages = ["ru.yandex.clickhouse:clickhouse-jdbc:0.2.4"]
    if additional_packages is not None and isinstance(additional_packages, list):
        spark_packages.extend(additional_packages)
    elif additional_packages is not None and isinstance(additional_packages, str):
        spark_packages.append(additional_packages)
    elif additional_packages is not None:
        raise Exception(f"Parameter additional_packages should be a list of packages or a package as a string. "
                        f"Ex: ['ru.yandex.clickhouse:clickhouse-jdbc:0.2.4', 'org.apache.hadoop:hadoop-aws:3.2.2'] "
                        f"or 'org.apache.hadoop:hadoop-aws:3.2.2' "
                        f"Note that ru.yandex.clickhouse:clickhouse-jdbc:0.2.4 is provided by default. "
                        f"The s3 package is controlled by use_emr and set_s3. You do not need to provide them in additional_packages parameter.")

    if use_emr:
        # Don't set up master parameter here in cluster mode (very important)
        conf = (
                SparkConf().setAppName(app_name)
                           .set('spark.driver.maxResultSize', '5g')
                           .set("spark.sql.orc.filterPushdown", "true")
                           .set("spark.jars.packages", ','.join(spark_packages))
               )
    else:
        if spark_master is None:
            raise Exception(f"Spark master is not provided!")
        if hadoop_aws_version is None and set_s3:
            raise Exception(f"Hadoop version for AWS dependency is not provided!")
        if (aws_s3_access_key_id is None or aws_s3_access_key_secret is None) and set_s3:
            raise Exception(f"AWS S3 credential is not provided!")

        conf = (SparkConf().setAppName(app_name)
                           .setMaster(spark_master)
                           .set("spark.sql.execution.arrow.pyspark.enabled", "true")
                           .set("spark.sql.broadcastTimeout", "36000")
                           .set('spark.driver.maxResultSize', '10g')
                           .set('spark.driver.memory', '12g')
                           .set('spark.executor.memory', '16g')
                           # .set("spark.sql.orc.filterPushdown", "true")
                )

        if set_s3:
            spark_packages.append(f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}")
            conf = (conf.set("spark.jars.packages", ','.join(spark_packages))
                        .set("spark.hadoop.fs.s3a.access.key", aws_s3_access_key_id)
                        .set("spark.hadoop.fs.s3a.secret.key", aws_s3_access_key_secret)
                        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                        .set("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
                        .set("spark.hadoop.fs.s3a.fast.upload", "true")
                        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))
        else:
            conf = conf.set("spark.jars.packages", ','.join(spark_packages))

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark