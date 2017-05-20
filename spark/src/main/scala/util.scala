package com.it21learning.etl

import org.apache.spark._

object Util {
    //the date-time format
    val fmtDateTime = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    //calculate the days
    def getDays(t1:String, t2:String):Int = {
        //Note:
        //t1 is in the format of yyyy-MM-dd hh:mm:ss ******
        //t2 is in the format of yyyy-MM-ddThh:mm:ss ******
        var d1 = fmtDateTime.parse(t1.substring(0, 19))
        var d2 = fmtDateTime.parse(t2.substring(0, 10).concat(" ").concat(t2.substring(11, 19)))
        //difference in days
        ((d2.getTime() - d1.getTime())/(24 * 60 * 60 * 1000) + 1).toInt
    }    

    //convert a data-frame row into a CSV string
    def toCSV(r:org.apache.spark.sql.Row):String = {
        //concatenation
        r.mkString(",")
    }
}
