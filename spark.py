from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
import timeit
import sys

if os.environ["COUNT"] == "0":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '8g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "1"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])

if os.environ["COUNT"] == "1":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '8g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
        "numPartitions": 2,
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 2 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "2"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])

if os.environ["COUNT"] == "2":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '10g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
        "numPartitions": 4,
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 4 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "3"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])

if os.environ["COUNT"] == "3":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '8g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
        "numPartitions": 6,
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 6 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "4"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])

if os.environ["COUNT"] == "4":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '8g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
        "numPartitions": 8,
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 8 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "5"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])

if os.environ["COUNT"] == "5":
    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.postgresql:postgresql:42.2.4')
    conf.set('spark.driver.memory', '8g')
    conf.set('spark.driver.maxResultSize', '10g')

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config(conf=conf) \
        .getOrCreate()

    db_host = "127.0.0.1"
    db_port = 5432
    table_name = "mock_table"
    db_name = "mock_db_1m"
    db_url = "jdbc:postgresql://{}:{}/{}".format(db_host, db_port, db_name)

    options = {
        "url": db_url,
        "dbtable": table_name,
        "user": "user",
        "password": "password",
        "driver": "org.postgresql.Driver",
        "numPartitions": 10,
    }
    df = spark.read.format('jdbc').options(**options).load()
    with open("report.txt", "a") as file:
        file.write("Read 1M, 10 numPartitions, time: %s\n" % timeit.timeit(df.collect, number=50))
    with open("report.txt", "a") as file:
        file.write("Read 1M, 1 numPartitions, time: %s\n" % timeit.timeit(lambda: df.write.format('jdbc').options(**options).save("overwrite"), number=50))
    os.environ["COUNT"] = "6"
    os.execv(sys.executable,
             [sys.executable, os.path.join(sys.path[0], __file__)] + sys.argv[1:])