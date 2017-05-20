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
class UserRow {
    //user id
    private var _userId:String = ""
    //read property
    def UserID = _userId
    //set property
    def UserID_=(id:String) = {
        //set
        _userId = id
    }

    //birth year
    private var _birthYear:String = "2017"
    //read property
    def BirthYear = _birthYear
    //set property
    def BirthYear_=(birth_year:String) = {
        //set
        _birthYear = birth_year
    }

    //gender
    private var _gender:String = ""
    //read property
    def Gender = _gender
    //set property
    def Gender_=(gender:String) = {
        //set
        _gender = gender
    }

    //location
    private var _location:String = ""
    //read property
    def Location = _location
    //set property
    def Location_=(location:String) = {
        //set
        _location = location
    }
 
    //locale
    private var _locale:String = ""
    //read property
    def Locale = _locale
    //set property
    def Locale_=(locale:String) = {
        //set
        _locale = locale
    }

    //time_zone
    private var _time_zone:String = ""
    //read property
    def TimeZone = _time_zone
    //set property
    def TimeZone_=(time_zone:String) = {
        //set
        _time_zone = time_zone
    }

    //joined_at
    private var _joined_at:String = ""
    //read property
    def JoinedAt = _joined_at
    //set property
    def JoinedAt_=(joined_at:String) = {
        //set
        _joined_at = joined_at
    }
}

@serializable
class User() extends Base[UserRow]("users") {
    //row key
    val _cUserId = "user_id"

    //column families
    val _cfProfile = "profile"
    val _cfRegion = "region"
    val _cfRegistration = "registration"
    //columns
    val _cBirthYear = "birth_year"
    val _cGender = "gender"
    val _cLocale = "locale"
    val _cLocation = "location"
    val _cTimeZone = "time_zone"
    val _cJoinedAt = "joined_at"

    //column families
    val cfProfile = Bytes.toBytes(_cfProfile)
    val cfRegion = Bytes.toBytes(_cfRegion)
    val cfRegistration = Bytes.toBytes(_cfRegistration)
    //columns
    val cBirthYear = Bytes.toBytes(_cBirthYear)
    val cGender = Bytes.toBytes(_cGender)
    val cLocale = Bytes.toBytes(_cLocale)
    val cLocation = Bytes.toBytes(_cLocation)
    val cTimeZone = Bytes.toBytes(_cTimeZone)
    val cJoinedAt = Bytes.toBytes(_cJoinedAt)

    //get the schema of the current entity
    override def getSchema():StructType = {
        //create struct type
        StructType(StructField(_cUserId, StringType, true) 
            :: StructField(_cBirthYear, StringType, false) 
            :: StructField(_cGender, StringType, false)
            :: StructField(_cLocale, StringType, false)
            :: StructField(_cLocation, StringType, false)
            :: StructField(_cTimeZone, StringType, false)
            :: StructField(_cJoinedAt, StringType, false) :: Nil)
    }

    //parse a row
    override def parse(k:ImmutableBytesWritable, v:Result):UserRow = {    
        //create a UserRow
        var ur = new UserRow()
        //set
        ur.UserID = Bytes.toString(k.copyBytes())                                     //row key - user id
        ur.BirthYear = Bytes.toString(v.getValue(cfProfile, cBirthYear))              //birth year
        ur.Gender = Bytes.toString(v.getValue(cfProfile, cGender))                    //gender
        ur.Locale = Bytes.toString(v.getValue(cfRegion, cLocale))                     //locale
        ur.Location = Bytes.toString(v.getValue(cfRegion, cLocation))                 //location
        ur.TimeZone = Bytes.toString(v.getValue(cfRegion, cTimeZone))                 //time zone
        ur.JoinedAt = Bytes.toString(v.getValue(cfRegistration, cJoinedAt))           //joined_at
        //return
        ur
    }
}
