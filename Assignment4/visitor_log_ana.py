#!/usr/bin/python
#-----------------------------------------------------
# This is a White House Visitor Log analysis in PySpark.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Shenghua Yue
#-------------------------------------------------------


from __future__ import print_function 
import sys 
from pyspark.sql import SparkSession 

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: visitor_log_ana.py  <input-file>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("Visitor_Log_Ana")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    
    # read input and create an DF
    df = spark.read.format('csv').option('header','true').option('interSchema','true').load(input_path)
    print("Total number of records: ", df.count())
    print("Records examples: ", df.show(3))

    
    # remove the unuseful columns
    df_select = df.select('NAMELAST','NAMEFIRST', 'NAMEMID', 'visitee_namelast', 'visitee_namefirst')
    print("Records examples: ", df_select.show(3))    
    
    
    # remove invalid records and convert to lowercase 
    # a. if a visitor's last name (i.e., NAMELAST) is null/empty, then drop that record
    # b. if visitee_namelast is null/empty, then drop that record
    # c. convert all characters to lowercase letters
    # d. If a given visitor's lastname or visitee's last name has non-English characters thenthat record is dropped from all calculations.
    df_filter = df_select.na.drop(subset=['NAMELAST','visitee_namelast'])
    df_lower = df_filter.withColumn('NAMELAST', lower(col('NAMELAST'))) \
                        .withColumn('NAMEFIRST', lower(col('NAMEFIRST'))) \
                        .withColumn('NAMEMID', lower(col('NAMEMID'))) \
                        .withColumn('visitee_namelast', lower(col('visitee_namelast'))) \
                        .withColumn('visitee_namefirst', lower(col('visitee_namefirst')))
    df_letter = df_lower.filter(df_lower.NAMELAST.rlike('^[a-z]+$')).filter(df_lower.visitee_namelast.rlike('^[a-z]+$'))
    print("Total number of records after dropping the invalid records: ", df_letter.count())
    print("Records examples: ", df_letter.show())
    
    # create table visitlog
    df_letter.createOrReplaceTempView('visitlog')

    # Q1: The 10 most frequent visitors (NAMELAST, NAMEFIRST, NAMEMID) to the White House. 
    # <visitor> <frequency>
    df_mf_visitors = spark.sql('select NAMELAST, NAMEFIRST, NAMEMID, count(*) as frequency from visitlog group by NAMELAST, NAMEFIRST, NAMEMID order by frequency desc').limit(10)   
    print("Q1 The 10 most frequent visitors with frequency: ",  df_mf_visitors.show())
  
    
    ## Q2: The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    ## <visitee> <frequency>
    df_mf_visitee = spark.sql('select visitee_namelast, visitee_namefirst, count(*) as frequency from visitlog group by visitee_namelast, visitee_namefirst order by frequency desc').limit(10)
    print("Q2 The 10 most frequently visited people with frequency: ", df_mf_visitee.show())

    # Q3: The 10 most frequent visitor-visitee combinations.
    # <visitor-visitee> <frequency>
    df_mf_vv = spark.sql('select NAMELAST, NAMEFIRST, NAMEMID, visitee_namelast, visitee_namefirst, count(*) as frequency from visitlog group by NAMELAST, NAMEFIRST, NAMEMID, visitee_namelast, visitee_namefirst order by frequency desc').limit(10)   
    print("Q3 The 10 most frequent visitor-visitee combinations with frequency: ", df_mf_vv.show())
    
    # Q4: The number of records dropped.
    print("Q4 The number of records dropped: ", df.count() - df_letter.count())
    
    # done!
    spark.stop()

