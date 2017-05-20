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
class UserFriendRow {
    //user id
    private var _userId:String = ""
    //read property
    def UserID = _userId
    //set property
    def UserID_=(id:String) = {
        //set
        _userId = id
    }

    //friend id
    private var _friendId:String = ""
    //read property
    def FriendID = _friendId
    //set property
    def FriendID_=(id:String) = {
        //set
        _friendId = id
    }
}

@serializable
class UserFriend() extends Base[UserFriendRow]("user_friend") {
    //column families
    val _cfUserFriend = "uf"
    //columns
    val _cUserID = "user_id"
    val _cFriendID = "friend_id"

    //column families
    val cfUserFriend = Bytes.toBytes(_cfUserFriend)
    //columns
    val cUserID = Bytes.toBytes(_cUserID)
    val cFriendID = Bytes.toBytes(_cFriendID)

    //get the schema of the current entity
    override def getSchema():StructType = {
        //create struct type
        StructType(StructField(_cUserID, StringType, false) :: StructField(_cFriendID, StringType, false) :: Nil)
    }

    //parse a row
    override def parse(k:ImmutableBytesWritable, v:Result):UserFriendRow = {    
        //create an UserFriendRow
        var uf = new UserFriendRow()
        //set
        uf.UserID = Bytes.toString(v.getValue(cfUserFriend, cUserID))        //user id
        uf.FriendID = Bytes.toString(v.getValue(cfUserFriend, cFriendID))    //friend id
        //return
        uf
    }
}
