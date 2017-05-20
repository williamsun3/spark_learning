
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration

import com.it21learning.etl._

//the control object
object ETL_Train {
    //import etl-types
	import ETLType._

    //start entry
    def main(args:Array[String]) {
        //prepare train data
		ETLs.start(ETLType.prepareTrain, "Prepare-Train")
    }
}
