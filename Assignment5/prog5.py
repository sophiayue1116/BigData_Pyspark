#!/usr/bin/python
# spark-submit  --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11  prog5.py  p2p-Gnutella09.txt
#-----------------------------------------------------
# This is a GraphFrame analysis in PySpark.
#------------------------------------------------------
# Input Parameters:
#    argv[1]: String, input path
#-------------------------------------------------------
# @author Sophie Yue
#-------------------------------------------------------

from __future__ import print_function 
import sys 
from pyspark import SparkContext
from pyspark.sql import SparkSession, Row 
from graphframes import *

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: prog5.py  <input-file>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("Connected_Component_Ana")\
        .getOrCreate()

    #  sys.argv[0] is the name of the script.
    #  sys.argv[1] is the first parameter
    input_path = sys.argv[1]  
    print("input_path: {}".format(input_path))

    # read input and create an RDD<String>
    records = spark.sparkContext.textFile(input_path)
    records_txt = records.filter(lambda x: "#" not in x[0]) 
    
    # create Edge DF   
    temp_var = records_txt.map(lambda x: x.split("\t"))
    edges = temp_var.toDF(["src", "dst"])
    print("Total number of edges: ", edges.count())
    
    # create Node DF
    temp_var = records_txt.flatMap(lambda x: x.split("\t")).distinct()
    row = Row("id") 
    vertices = temp_var.map(row).toDF()
    print("Total number of vertices: ", vertices.count())

    # create Graph
    g = GraphFrame(vertices, edges)
    print(g)
    
    # create Connected Components
    spark.sparkContext.setCheckpointDir("graphframes_cps")
    ccg = g.connectedComponents()
 
    # find the Smallest and Largest Connected Component
    grouped = ccg.groupby('component').count()
    smallest = grouped.sort('count', ascending = True).take(1)
    print("The Smallest Connected Component: ", smallest)    
    largest = grouped.sort('count', ascending = False).take(1)
    print("The Largest Connected Component: ", largest)
   
    # done!
    spark.stop()

