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
class UserEventRow {
    //user id
    private var _userId:String = ""
    //read property
    def UserID = _userId
    //set property
    def UserID_=(userId:String) = {
        //set
        _userId = userId
    }

    //event id
    private var _eventId:String = ""
    //read property
    def EventID = _eventId
    //set property
    def EventID_=(eventId:String) = {
        //set
        _eventId = eventId
    }

    //invited
    private var _invited:String = ""
    //read property
    def Invited = _invited
    //set property
    def Invited_=(invited:String) = {
        //set
        _invited = invited
    }

    //time stamp
    private var _timeStamp:String = ""
    //read property
    def TimeStamp = _timeStamp
    //set property
    def TimeStamp_=(timeStamp:String) = {
        //set
        _timeStamp = timeStamp
    }

    //interested
    private var _interested:String = ""
    //read property
    def Interested = _interested
    //set property
    def Interested_=(interested:String) = {
        //set
        _interested = interested
    }
}

//the companion object for defining the time-stamp format
object UserEvent {
	//time-stamp format
    private var _fmtTimeStamp = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.*"
    
	//read only property
    def TimeStampFormat = _fmtTimeStamp
}

@serializable 
abstract class UserEvent(tableName:String) extends Base[UserEventRow](tableName) {
    //column families
    val _cfUserEvent = "eu"
    //columns
    val _cUser = "user"
    val _cEvent = "event"
    val _cInvited = "invited"
    val _cTimeStamp = "time_stamp"
    val _cInterested = "interested"
    //derived valid column
    val _cValid = "valid"

    //column families
    protected val cfUserEvent = Bytes.toBytes(_cfUserEvent)
    //columns
    protected val cUser = Bytes.toBytes(_cUser)
    protected val cEvent = Bytes.toBytes(_cEvent)
    protected val cInvited = Bytes.toBytes(_cInvited)
    protected val cTimeStamp = Bytes.toBytes(_cTimeStamp)
    protected val cInterested = Bytes.toBytes(_cInterested)

    //get the schema of the current entity
    override def getSchema():StructType = {
        //create struct type
        StructType(StructField(_cUser, StringType, true) 
            :: StructField(_cEvent, StringType, false) 
            :: StructField(_cInvited, StringType, false)
            :: StructField(_cTimeStamp, StringType, false)
            :: StructField(_cInterested, StringType, false) 
            :: StructField(_cValid, BooleanType, false) :: Nil)
    }
}
