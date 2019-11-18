# coding: utf-8

# In[ ]:

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

    # read input and create an RDD<String>
    records = spark.sparkContext.textFile(input_path)
    print("Total number of records (including the header): ", records.count())
    print("Records examples: ", records.take(3))

    # remove the header/first line/column names from the loaded file
    header = records.first()
    input_data = records.filter(lambda x: x != header)
    print("Total number of records (without the header): ", input_data.count())

    # convert all words to lowercase 
    # a. if a visitor's last name (i.e., NAMELAST) is null/empty, then drop that record
    # b. if visitee_namelast is null/empty, then drop that record
    rdd_filter = input_data.map(lambda x: x.lower().split(',')).filter(lambda x: x[0] and x[19])
    print("Total number of records after dropping the invalid records: ", rdd_filter.count())

    # Q1: The 10 most frequent visitors (NAMELAST, NAMEFIRST, NAMEMID) to the White House. 
    # <visitor> <frequency>
    rdd_mf_visitors = rdd_filter.map(lambda x: ((x[0],x[1],x[2]), 1)).reduceByKey(lambda a,b: a+b)   
    print("Q1 The 10 most frequent visitors with frequency: ",  rdd_mf_visitors.takeOrdered(10, key=lambda x: -x[1]))
    
    ## Q2: The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    ## <visitee> <frequency>
    rdd_mf_visitee = rdd_filter.map(lambda x: ((x[19],x[20]), 1)).reduceByKey(lambda a,b: a+b)
    print("Q2 The 10 most frequently visited people with frequency: ", rdd_mf_visitee.takeOrdered(10, key=lambda x: -x[1]))

    # Q3: The 10 most frequent visitor-visitee combinations.
    # <visitor-visitee> <frequency>
    rdd_mf_vv = rdd_filter.map(lambda x: ((x[0],x[1],x[2],x[19],x[20]), 1)).reduceByKey(lambda a,b: a+b)    
    print("Q3 The 10 most frequent visitor-visitee combinations with frequency: ", rdd_mf_vv.takeOrdered(10, key=lambda x: -x[1]))
    
    # Q4: The number of records dropped.
    print("Q4 The number of records dropped: ", records.count()- rdd_filter.count())
    
    # done!
    spark.stop()

