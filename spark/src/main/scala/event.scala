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
class EventRow {
    //event id
    private var _eventId:String = ""
    //read property
    def EventID = _eventId
    //set property
    def EventID_=(id:String) = {
        //set
        _eventId = id
    }

    //start time
    private var _startTime:String = ""
    //read property
    def StartTime = _startTime
    //set property
    def StartTime_=(st:String) = {
        //set
        _startTime = st
    }

    //city
    private var _city:String = ""
    //read property
    def City = _city
    //set property
    def City_=(city:String) = {
        //set
        _city = city
    }

    //state
    private var _state:String = ""
    //read property
    def State = _state
    //set property
    def State_=(state:String) = {
        //set
        _state = state
    }
 
    //zip
    private var _zip:String = ""
    //read property
    def Zip = _zip
    //set property
    def Zip_=(zip:String) = {
        //set
        _zip = zip
    }

    //country
    private var _country:String = ""
    //read property
    def Country = _country
    //set property
    def Country_=(country:String) = {
        //set
        _country = country
    }

    //latitude
    private var _latitude:String = ""
    //read property
    def Latitude = _latitude
    //set property
    def Latitude_=(latitude:String) = {
        //set
        _latitude = latitude
    }

    //longitude
    private var _longitude:String = ""
    //read property
    def Longitude = _longitude
    //set property
    def Longitude_=(longitude:String) = {
        //set
        _longitude = longitude
    }

    //user id
    private var _userId:String = ""
    //read property
    def UserID = _userId
    //set property
    def UserID_=(userId:String) = {
        //set
        _userId = userId
    }
}

object Event {
    //time-stamp format
    private var _fmtStartTime = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z"
    //read only property
    def StartTimeFormat = _fmtStartTime
}

@serializable
class Event() extends Base[EventRow]("events") {
    //row key
    val _cEventId = "event_id"

    //column families
    val _cfSchedule = "schedule"
    val _cfLocation = "location"
    val _cfCreator = "creator"
    //columns
    val _cStartTime = "start_time"
    val _cCity = "city"
    val _cState = "state"
    val _cZip = "zip"
    val _cCountry = "country"
    val _cLatitude = "latitude"
    val _cLongitude = "longitude"
    val _cUserId = "user_id"
    //derived valid column
    val _cValid = "valid"

    //column families
    val cfSchedule = Bytes.toBytes(_cfSchedule)
    val cfLocation = Bytes.toBytes(_cfLocation)
    val cfCreator = Bytes.toBytes(_cfCreator)
    //columns
    val cStartTime = Bytes.toBytes(_cStartTime)
    val cCity = Bytes.toBytes(_cCity)
    val cState = Bytes.toBytes(_cState)
    val cZip = Bytes.toBytes(_cZip)
    val cCountry = Bytes.toBytes(_cCountry)
    val cLatitude = Bytes.toBytes(_cLatitude)
    val cLongitude = Bytes.toBytes(_cLongitude)
    val cUserId = Bytes.toBytes(_cUserId)

    //get the schema of the current entity
    override def getSchema():StructType = {
        //create struct type
        StructType(StructField(_cEventId, StringType, true) 
            :: StructField(_cStartTime, StringType, false) 
            :: StructField(_cCity, StringType, false)
            :: StructField(_cState, StringType, false)
            :: StructField(_cZip, StringType, false)
            :: StructField(_cCountry, StringType, false)
            :: StructField(_cLatitude, FloatType, false)
            :: StructField(_cLongitude, FloatType, false)
            :: StructField(_cUserId, StringType, false) 
            :: StructField(_cValid, BooleanType, false) :: Nil)
    }

    //parse a row
    override def parse(k:ImmutableBytesWritable, v:Result):EventRow = {    
        //create an EventRow
        var er = new EventRow()
        //set
        er.EventID = Bytes.toString(k.copyBytes())                            //row key - event id
        er.StartTime = Bytes.toString(v.getValue(cfSchedule, cStartTime))     //start time
        er.City = Bytes.toString(v.getValue(cfLocation, cCity))               //city
        er.State = Bytes.toString(v.getValue(cfLocation, cState))             //state
        er.Zip = Bytes.toString(v.getValue(cfLocation, cZip))                 //zip
        er.Country = Bytes.toString(v.getValue(cfLocation, cCountry))         //country
        er.Latitude = Bytes.toString(v.getValue(cfLocation, cLatitude))       //latitude
        er.Longitude = Bytes.toString(v.getValue(cfLocation, cLongitude))     //longitude
        er.UserID = Bytes.toString(v.getValue(cfCreator, cUserId))            //user id
        //return
        er
    }
}
