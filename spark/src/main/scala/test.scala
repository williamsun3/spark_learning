package com.it21learning.etl

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

@serializable 
class Test() extends UserEvent("test") {
    //parse a row
    override def parse(k:ImmutableBytesWritable, v:Result):UserEventRow = {   
        //create a UserEventRow
        var tr = new UserEventRow()
        //set
        tr.UserID = Bytes.toString(v.getValue(cfUserEvent, cUser))              //user
        tr.EventID = Bytes.toString(v.getValue(cfUserEvent, cEvent))            //event
        tr.Invited = Bytes.toString(v.getValue(cfUserEvent, cInvited))          //invited
        tr.TimeStamp = Bytes.toString(v.getValue(cfUserEvent, cTimeStamp))      //time stamp
        tr.Interested = "0"                                                     //interested
        //return 
        tr
    }
}
