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
class EventAttendeeRow {
    //event id
    private var _eventId:String = ""
    //read property
    def EventID = _eventId
    //set property
    def EventID_=(id:String) = {
        //set
        _eventId = id
    }

    //user id
    private var _userId:String = ""
    //read property
    def UserID = _userId
    //set property
    def UserID_=(id:String) = {
        //set
        _userId = id
    }

    //attend type
    private var _attendType:String = ""
    //read property
    def AttendType = _attendType
    //set property
    def AttendType_=(at:String) = {
        //set
        _attendType = at
    }
}

@serializable
class EventAttendee() extends Base[EventAttendeeRow]("event_attendee") {
    //column families
    val _cfEventAttendee = "euat"
    //columns
	val _cEventID = "event_id"
    val _cUserID = "user_id"
    val _cAttendType = "attend_type"

    //column families
    val cfEventAttendee = Bytes.toBytes(_cfEventAttendee)
    //columns
	val cEventID = Bytes.toBytes(_cEventID)
    val cUserID = Bytes.toBytes(_cUserID)
    val cAttendType = Bytes.toBytes(_cAttendType)

    //get the schema of the current entity
    override def getSchema():StructType = {
        //create struct type
        StructType(StructField(_cEventID, StringType, false) :: StructField(_cUserID, StringType, false) :: StructField(_cAttendType, StringType, false) :: Nil)
    }

    //parse a row
    override def parse(k:ImmutableBytesWritable, v:Result):EventAttendeeRow = {    
        //create an EventAttendeeRow
        var ea = new EventAttendeeRow()
        //set
		ea.EventID = Bytes.toString(v.getValue(cfEventAttendee, cEventID))   //event id
        ea.UserID = Bytes.toString(v.getValue(cfEventAttendee, cUserID))        //user id
        ea.AttendType = Bytes.toString(v.getValue(cfEventAttendee, cAttendType))    //friend id
        //return
        ea
    }
}
