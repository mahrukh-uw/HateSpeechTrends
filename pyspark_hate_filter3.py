# Hate content filtering by Mahrukh (mahrukh@uw.edu)
#version 3
import configparser
import pyspark.sql.functions as psf
from pyspark.sql.functions import col, size
from pyspark.sql.functions import length, lower
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys
import re
from dbconnector import CassandraConnector as cassandra # Loading my own cassandra connector

conf = SparkConf().setMaster("local").setAppName("pyspark-hatespeech").set("spark.jars.packages", \
            "org.apache.hadoop:hadoop-aws:2.7.3,com.datastax.spark:spark-cassandra-connector_2.11:2.4.1") 
sc = SparkContext(conf=conf)
spark=SQLContext(sc)

s3_path="s3n://mahrukh-s3/tweets/tweets/*.txt"
#s3_path="s3n://mahrukh-s3/tweets/tweets/Twitter2.tsv"

lexicon_file="refined_ngram_dict2.csv"

def get_data(s3_path):
	aws_profile = "myaws"
	config = configparser.ConfigParser()
	access_id = "..."
	access_key ="..."
	hadoop_conf = spark._jsc.hadoopConfiguration()
	hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
	hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
	hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)
	sdf = spark.read.options(header=True,sep='\t').csv(s3_path)
	sdf.head()
	return sdf

def get_data2(s3_path):
    aws_profile = "myaws"
    config = configparser.ConfigParser()
    dc={}
    with open('/home/ubuntu/.aws/credentials2') as fin:
        for line in fin:
            line=line.strip()
            key,value=line.split(':')
            dc[key]=value
    access_id = dc['access_id']
    access_key =dc['access_key']
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)
    sdf = spark.read.options(header=True,sep='\t').csv(s3_path)
    sdf.head()
    return sdf



def get_lexicon(filename):
    lst=[]
    with open(filename) as fin:
        for line in fin:
            line=line.strip()
            lst.append(line)
    print('Length of lexicon is ', len(lst))
    return lst

def build_regex(keywords):
    res = '('
    for key in keywords:
        if re.match('^[a-zA-Z]+', key):
            res += '\\b' + key + '\\b|'
    res = res[0:len(res) - 1] + ')'
    print(res)
    return res


def get_matching_string(line, regex):
    #print('******************', line, regex[:5])
    matches = re.findall(regex, line)
    return matches if matches else None




def text_lookup(df,lexicon):
    col1='text'
    col2='matched_text'
    df = df.withColumn(col1, F.lower(F.col(col1)))
    df2=df.withColumn(
        col2, 
        psf.regexp_extract(col1, '(?=^|\s)(' + '|'.join(lexicon) + ')(?=\s|$)', 0))
    return df2

def text_lookup2(df,lexicon):
    col1='text'
    col2='matched_text'
    df = df.filter(df.text.isNotNull())
    udf_func = udf(lambda line, regex: get_matching_string(line, regex),ArrayType(StringType()))
    #df2 = df.withColumn('matched_text', udf_func(df['text'], F.lit(build_regex(lexicon)))).withColumn('count', F.size('matched_text'))
    df2 = df.withColumn(col2, udf_func(df[col1], F.lit(build_regex(lexicon)))).withColumn('count', F.size(col2))

    return df2

# 1 Read data from s3
print('Reading data from s3')
df=get_data2(s3_path)
# 2 Read Lexicon
print('Reading lexicons')
lexicon=get_lexicon(lexicon_file)

# 3 Lookup text
print('Applying text filters')
df2=text_lookup2(df,lexicon)

# 4 Filter rows where hate content was found
print('Showing rows where text was filtered')
result=df2.filter(df2['count']>0)
result.show()

# 5 renaming columns
#id|    userid|           username|                text|            hashtags|      date|retweet_count|label|matched_text|count
result= result.select(col("id"),col("userid"),col("username"),col("hashtags"),col("date"),col("retweet_count"),col("label"), col("text").alias("inputtext"), col("matched_text").alias("match"))
result.show()

# 5 write to cassandra
print('Writing to cassandra')

cassandra.write_df(result, table_name="textmatch2", keyspace_name="hatespeech", show=True)

print('Calculate daily count of hate tweets')
count_by_dates=result.filter(result.date.isNotNull()).cube('date').count()
count_by_dates=count_by_dates.filter(count_by_dates.date.isNotNull())
print(count_by_dates.count())
cassandra.write_df(count_by_dates, table_name="dailytrend", keyspace_name="hatespeech", show=True)

print('Calculate daily count of overall tweets')
overall_by_dates=df2.filter(df2.date.isNotNull()).cube('date').count()
overall_by_dates=overall_by_dates.filter(overall_by_dates.date.isNotNull())
print(overall_by_dates.count())
cassandra.write_df(overall_by_dates, table_name="overalldailytrend", keyspace_name="hatespeech", show=True)

