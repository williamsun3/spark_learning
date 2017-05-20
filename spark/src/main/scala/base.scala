package com.it21learning.etl

import reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration

@serializable
abstract class Base[T:ClassTag](tableName:String) {
    //set the table name
    protected val _tableName = tableName

    //get the schema of the current entity
    def getSchema():StructType

    //load data from HBase
    def load(hbCfg:Configuration, sc:SparkContext):RDD[T] = {
        //set the table name
        hbCfg.set(TableInputFormat.INPUT_TABLE, _tableName)
        //load the events
        val rowsRDD = sc.newAPIHadoopRDD(hbCfg, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
        //parse
        rowsRDD.map({ case(k, v) => parse(k, v) })
    }
   
    //parse a row
    def parse(k:ImmutableBytesWritable, v:Result):T
}
