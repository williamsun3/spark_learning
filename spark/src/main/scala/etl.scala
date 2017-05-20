package com.it21learning.etl

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

//the control object
object ETLs {
    //import etl-types
	import ETLType._

    //start entry
    def start(etlType:ETLType, appName:String) {
        //create the spark context
        val sc = new SparkContext(new SparkConf().setAppName(appName))
        //the sql context
        val sqlCtx = new SQLContext(sc)
        import sqlCtx.implicits._

        //hbase configuration
        val hbc = HBaseConfiguration.create()
        hbc.set("hbase.zookeeper.property.clientPort", "2181")
        hbc.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com")
        hbc.set("zookeeper.znode.parent", "/hbase-unsecure")

        //prepare train data
        etl(sc, sqlCtx, hbc, etlType)
    }
	
    //prepare Train-Data
    def etl(sc:SparkContext, sqlCtx:SQLContext, hbc:Configuration, etlType:ETLType):Unit = {
	    //prepare user-event
		var ue: UserEvent = null
		//check
		etlType match {
		    //etl for train data
			case ETLType.prepareTrain => {
				//train
				ue = new Train()
			}
			
			//etl for test data
			case ETLType.prepareTest => {
				//test
				ue = new Test()
			}
		}

		//check
		if ( ue != null ) {
			//load
			val ueRDD = ue.load( hbc, sc )
			//events
			val event:Event = new Event()
			//load
			val eventsRDD = event.load( hbc, sc )
			//prepare user-event
			val user_event = prepareUserEvent(sqlCtx, ueRDD, ue.getSchema(), eventsRDD, event.getSchema())
			//persist
			user_event.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](user_event.schema.treeString), 1).saveAsTextFile("/user/events/tmp/user_event/schema")
			user_event.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/user_event/data")

			//user friend
			val uf:UserFriend = new UserFriend()
			//load
			val ufRDD = uf.load( hbc, sc )
			//prepare user-friend
			val user_friend = prepareUserFriend(sqlCtx, ufRDD, uf.getSchema())
			//persist
			user_friend.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](user_friend.schema.treeString), 1).saveAsTextFile("/user/events/tmp/user_friend/schema")
			user_friend.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/user_friend/data")
			//prepare user-friend count
			val user_friend_count = countUserFriends(sqlCtx)
			//persist
			user_friend_count.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](user_friend_count.schema.treeString), 1).saveAsTextFile("/user/events/tmp/user_friend_count/schema")
			user_friend_count.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/user_friend_count/data")
			
			//event attendee
			val ea:EventAttendee = new EventAttendee()
			//load
			val eaRDD = ea.load( hbc, sc )
			//prepare event-attendee
			val event_attendee = prepareEventAttendee(sqlCtx, eaRDD, ea.getSchema())
			//persist
			event_attendee.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](event_attendee.schema.treeString), 1).saveAsTextFile("/user/events/tmp/event_attendee/schema")
			event_attendee.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/event_attendee/data")
			
			//check if event creator is a friend
			val event_creator_is_friend = checkEventCreatorIsFriend(sqlCtx)
            //persist
			event_creator_is_friend.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](event_creator_is_friend.schema.treeString), 1).saveAsTextFile("/user/events/tmp/event_creator_is_friend/schema")
			event_creator_is_friend.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/event_creator_is_friend/data")
			
			//calculate the user attend status
			val user_attend_status = calculateUserAttendStatus(sqlCtx)
            //persist
			user_attend_status.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](user_attend_status.schema.treeString), 1).saveAsTextFile("/user/events/tmp/user_attend_status/schema")
			user_attend_status.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/user_attend_status/data")
			
			//calculate friend attend status
			val friend_attend_status = calculateFriendAttendStatus(sqlCtx)
            //persist
			friend_attend_status.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](friend_attend_status.schema.treeString), 1).saveAsTextFile("/user/events/tmp/friend_attend_status/schema")
			friend_attend_status.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/friend_attend_status/data")

			//calculate friend attend summary
			val friend_attend_summary = calculateFriendAttendSummary(sqlCtx)
            //persist
			friend_attend_summary.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](friend_attend_summary.schema.treeString), 1).saveAsTextFile("/user/events/tmp/friend_attend_summary/schema")
			friend_attend_summary.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/friend_attend_summary/data")

			//calculate friend attend percentage
			val friend_attend_percentage = calculateFriendAttendPercentage(sqlCtx)
            //persist
			friend_attend_percentage.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](friend_attend_percentage.schema.treeString), 1).saveAsTextFile("/user/events/tmp/friend_attend_percentage/schema")
			friend_attend_percentage.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/friend_attend_percentage/data")

			//users
			val user:User = new User()
			//load
			val userRDD = user.load( hbc, sc )
			//prepare users
			val user_friend_event = prepareUserFriendEvent(sqlCtx, userRDD, user.getSchema())
            //persist
			user_friend_event.cache()
			//convert to CSV, and save
			sc.parallelize(List[String](user_friend_event.schema.treeString), 1).saveAsTextFile("/user/events/tmp/user_friend_event/schema")
			user_friend_event.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/tmp/user_friend_event/data")
			
			//load locale
			val localeRows = sc.textFile("/user/events/locale.txt").map(line => line.split("\t")).map(t => org.apache.spark.sql.Row(t(0), t(1)))
			//data frame
			val locales = sqlCtx.createDataFrame(localeRows, StructType(StructField("locale_id", StringType, true) :: StructField("locale", StringType, false) :: Nil))
			//register
			locales.registerTempTable("locale")
			//join with user friend event
			val data = sqlCtx.sql("SELECT ufe.interested, ufe.user, ufe.event, CASE WHEN l.locale_id IS NOT NULL THEN l.locale_id ELSE '0' END AS locale, ufe.gender, ufe.age, ufe.view_ahead_days, ufe.event_creator_is_friend, CASE WHEN ufe.invited_friends_percentage IS NOT NULL THEN ufe.invited_friends_percentage ELSE 0.0 END AS invited_friends_percentage, CASE WHEN ufe.attended_friends_percentage IS NOT NULL THEN ufe.attended_friends_percentage ELSE 0.0 END AS attended_friends_percentage, CASE WHEN ufe.not_attended_friends_percentage IS NOT NULL THEN ufe.not_attended_friends_percentage ELSE 0.0 END AS not_attended_friends_percentage, CASE WHEN ufe.maybe_attended_friends_percentage IS NOT NULL THEN ufe.maybe_attended_friends_percentage ELSE 0.0 END AS maybe_attended_friends_percentage, ufe.invited FROM user_friend_event ufe LEFT JOIN locale l ON ufe.locale = l.locale")
			//check
			etlType match {
				//etl for train data
				case ETLType.prepareTrain => {
					//convert to CSV, and save
					sc.parallelize(List[String](data.schema.treeString), 1).saveAsTextFile("/user/events/train/schema")
					data.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/train/data")
				}
				
				//etl for test data
				case ETLType.prepareTest => {
					//convert to CSV, and save
					sc.parallelize(List[String](data.schema.treeString), 1).saveAsTextFile("/user/events/test/schema")
					data.rdd.map(r => Util.toCSV(r)).saveAsTextFile("/user/events/test/data")
				}
			}
		}
    }

	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //prepare user friend event
	//Result:
	//  - Table Registered: user_friend_event
	//  - Table Schema: interested, user, event, locale, gender, age, view_ahead_days, event_creator_is_friend, invited_friends_count, attended_friends_count, not_attended_friends_count, .maybe_attended_friends_count, invited_friends_percentage, attended_friends_percentage, not_attended_friends_percentage, maybe_attended_friends_percentage, invited
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	def prepareUserFriendEvent(sc:SQLContext, userRows:RDD[UserRow], userSchema:StructType):DataFrame = {
	    //import
        import sc.implicits._

        //user rows
        val users = userRows.map(t => org.apache.spark.sql.Row(t.UserID, t.BirthYear, t.Gender, t.Locale, t.Location, t.TimeZone, t.JoinedAt))
        //data-frame
        var dfUsers = sc.createDataFrame(users, userSchema)
        //register train table 
        dfUsers.registerTempTable("users")
		
        val ageFun: (String => Int) = (birth_year:String) => if (birth_year forall Character.isDigit) (2017 - birth_year.toInt) else 0
		//user friend event
		val user_friend_event = sc.sql("SELECT fap.user_interested AS interested, u.user_id AS user, fap.event_id AS event, u.locale, CASE WHEN u.gender = 'male' THEN 1 ELSE 0 END AS gender, CASE WHEN u.birth_year IS NOT NULL THEN u.birth_year ELSE '2017' END AS birth_year, fap.view_ahead_days, fap.event_creator_is_friend, fap.invited_friends_count, fap.attended_friends_count, fap.not_attended_friends_count, fap.maybe_attended_friends_count, fap.invited_friends_percentage, fap.attended_friends_percentage, fap.not_attended_friends_percentage, fap.maybe_attended_friends_percentage, fap.user_invited AS invited FROM users u INNER JOIN friend_attend_percentage fap ON u.user_id = fap.user_id")
            .withColumn("age", callUDF(ageFun, IntegerType, col("birth_year")))
		//register
		user_friend_event.registerTempTable("user_friend_event")
		//return
		user_friend_event
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //calculate friend attend percentage
	//Result:
	//  - Table Registered: friend_attend_percentage
	//  - Table Schema: user_id, event_id, view_ahead_days, event_creator_is_friend, user_friend_count, user_invited, user_interested, invited_friends_count, invited_friends_percentage, attended_friends_count, attended_friends_percentage, not_attended_friends_count, not_attended_friends_percentage, maybe_attended_friends_count, maybe_attended_friends_percentage
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	def calculateFriendAttendPercentage(sc:SQLContext):DataFrame = {
	    //user_friend_summary
		val friend_attend_percentage = sc.sql("SELECT ecif.user_id, ecif.event_id, ecif.view_ahead_days, ecif.event_creator_is_friend, ecif.user_friend_count, ecif.user_invited, ecif.user_interested, fas.invited_friends_count, CASE WHEN ecif.user_friend_count != 0 THEN fas.invited_friends_count*100/ecif.user_friend_count ELSE 0 END AS invited_friends_percentage, fas.attended_friends_count, CASE WHEN ecif.user_friend_count != 0 THEN fas.attended_friends_count*100/ecif.user_friend_count ELSE 0 END AS attended_friends_percentage, fas.not_attended_friends_count, CASE WHEN ecif.user_friend_count != 0 THEN fas.not_attended_friends_count*100/ecif.user_friend_count ELSE 0 END AS not_attended_friends_percentage, fas.maybe_attended_friends_count, CASE WHEN ecif.user_friend_count != 0 THEN fas.maybe_attended_friends_count*100/ecif.user_friend_count ELSE 0 END AS maybe_attended_friends_percentage FROM event_creator_is_friend ecif LEFT JOIN friend_attend_summary fas ON ecif.user_id = fas.user_id AND ecif.event_id = fas.event_id")
        //register
        friend_attend_percentage.registerTempTable("friend_attend_percentage")
		//return
		friend_attend_percentage
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //calculate friend attend summary
	//Result:
	//  - Table Registered: friend_attend_summary
	//  - Table Schema: user_id, event_id, invited_friends_count, attended_friends_count, not_attended_friends_count, maybe_attended_friends_count
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	def calculateFriendAttendSummary(sc:SQLContext):DataFrame = {
		//friend attend summary
		val friend_attend_summary = sc.sql("SELECT user_id, event_id, SUM(invited) AS invited_friends_count, SUM(attended) AS attended_friends_count, SUM(not_attended) AS not_attended_friends_count, SUM(maybe_attended) AS maybe_attended_friends_count FROM friend_attend_status WHERE event_id IS NOT NULL GROUP BY user_id, event_id")
		//register
		friend_attend_summary.registerTempTable("friend_attend_summary")
		//return
		friend_attend_summary
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //calculate friend attend status
	//Result:
	//  - Table Registered: friend_attend_status
	//  - Table Schema: user_id, friend_id, event_id, invited, attended, not_attended, maybe_attended
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def calculateFriendAttendStatus(sc:SQLContext):DataFrame = {
	    //friend attend
		val friend_attend_status = sc.sql("SELECT uf.user_id, uf.friend_id, uas.event_id, uas.invited, uas.attended, uas.not_attended, uas.maybe_attended FROM user_friend uf LEFT JOIN user_attend_status uas ON uf.friend_id = uas.user_id")
		//register
		friend_attend_status.registerTempTable("friend_attend_status")
		//return
		friend_attend_status
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //calculate user attend status
	//Result:
	//  - Table Registered: user_attend_status
	//  - Table Schema: event_id, user_id, invited, attended, not_attended, maybe_attended
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	def calculateUserAttendStatus(sc:SQLContext):DataFrame = {
	    //invited vs. attended
		val invited_yes = sc.sql("SELECT CASE WHEN i.event_id IS NOT NULL THEN i.event_id ELSE y.event_id END AS event_id, CASE WHEN i.attend_user_id IS NOT NULL THEN i.attend_user_id ELSE y.attend_user_id END AS attend_user_id, CASE WHEN i.attend_user_id IS NOT NULL THEN 1 ELSE 0 END AS invited, CASE WHEN y.attend_user_id IS NOT NULL THEN 1 ELSE 0 END AS attended FROM event_invited_attendee i FULL JOIN event_yes_attendee y ON i.event_id = y.event_id AND i.attend_user_id = y.attend_user_id")
		//register
		invited_yes.registerTempTable("user_invited_yes")
		
		//combine with not-attended
		val invited_yes_no = sc.sql("SELECT CASE WHEN iy.event_id IS NOT NULL THEN iy.event_id ELSE n.event_id END AS event_id, CASE WHEN iy.attend_user_id IS NOT NULL THEN iy.attend_user_id ELSE n.attend_user_id END AS attend_user_id, CASE WHEN iy.invited IS NOT NULL THEN iy.invited ELSE 0 END AS invited, CASE WHEN iy.attended IS NOT NULL THEN iy.attended ELSE 0 END AS attended, CASE WHEN n.attend_user_id IS NOT NULL THEN 1 ELSE 0 END AS not_attended FROM user_invited_yes iy FULL JOIN event_no_attendee n ON iy.event_id = n.event_id AND iy.attend_user_id = n.attend_user_id")
        //register 
        invited_yes_no.registerTempTable("user_invited_yes_no")

		//combine with maybe_attended
		val user_attend_status = sc.sql("SELECT CASE WHEN iyn.event_id IS NOT NULL THEN iyn.event_id ELSE m.event_id END AS event_id, CASE WHEN iyn.attend_user_id IS NOT NULL THEN iyn.attend_user_id ELSE m.attend_user_id END AS user_id, CASE WHEN iyn.invited IS NOT NULL THEN iyn.invited ELSE 0 END AS invited, CASE WHEN iyn.attended IS NOT NULL THEN iyn.attended ELSE 0 END AS attended, CASE WHEN iyn.not_attended IS NOT NULL THEN iyn.not_attended ELSE 0 END AS not_attended, CASE WHEN m.attend_user_id IS NOT NULL THEN 1 ELSE 0 END AS maybe_attended FROM user_invited_yes_no iyn FULL JOIN event_maybe_attendee m ON iyn.event_id = m.event_id AND iyn.attend_user_id = m.attend_user_id")
        //register 
        user_attend_status.registerTempTable("user_attend_status")
		
		//return 
		user_attend_status
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //check if an event creator is a friend of a user
	//Result:
	//  - Table Registered: event_creator_is_friend
	//  - Table Schema: user_id, event_id, view_ahead_days, event_creator_is_friend, user_invited, user_interested, user_friend_count
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def checkEventCreatorIsFriend(sc:SQLContext):DataFrame = {
		//check if event creator is a friend
		val event_creator_is_friend = sc.sql("SELECT ue.user_id, ue.event_id, ue.view_ahead_days, CASE WHEN uf.friend_id IS NOT NULL THEN 1 ELSE 0 END AS event_creator_is_friend, ue.user_invited, ue.user_interested, CASE WHEN ufc.friend_count IS NOT NULL THEN ufc.friend_count ELSE 0 END AS user_friend_count FROM user_event ue LEFT JOIN user_friend uf ON ue.user_id = uf.user_id AND ue.event_creator = uf.friend_id LEFT JOIN user_friend_count ufc ON ue.user_id = ufc.user_id")
		//register
		event_creator_is_friend.registerTempTable("event_creator_is_friend")	
		//return
		event_creator_is_friend
    }	
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //prepare event attendee
	//Result:
	//  - Table Registered: event_attendee
	//  - Table Schema: event_id, user_id, attend_type
	//
	// Additional Tables Registered: event_yes_attendee, event_maybe_attendee, event_invited_attendee, event_no_attendee
	// Additional Tables Schema: event_id, attend_user_id
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def prepareEventAttendee(sc:SQLContext, eaRows:RDD[EventAttendeeRow], eaSchema:StructType):DataFrame = {
	    //import
        import sc.implicits._

		//event-attendee data-frame
		val event_attendee = sc.createDataFrame(eaRows.map(t => org.apache.spark.sql.Row(t.EventID, t.UserID, t.AttendType)), eaSchema)
		//register temp table
		event_attendee.registerTempTable("event_attendee")
		
		//attend-type: yes
		val event_yes_attendee = sc.sql("SELECT event_id, user_id AS attend_user_id FROM event_attendee WHERE attend_type = 'yes'")
		//register
		event_yes_attendee.registerTempTable("event_yes_attendee")

		//attend-type: maybe
		val event_maybe_attendee = sc.sql("SELECT event_id, user_id AS attend_user_id FROM event_attendee WHERE attend_type = 'maybe'")
		//register
		event_maybe_attendee.registerTempTable("event_maybe_attendee")

		//attend-type: invited
		val event_invited_attendee = sc.sql("SELECT event_id, user_id AS attend_user_id FROM event_attendee WHERE attend_type = 'invited'")
		//register
		event_invited_attendee.registerTempTable("event_invited_attendee")
		
		//attend-type: no
		val event_no_attendee = sc.sql("SELECT event_id, user_id AS attend_user_id FROM event_attendee WHERE attend_type = 'no'")
		//register
		event_no_attendee.registerTempTable("event_no_attendee")

		//return
		event_attendee
    }
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //prepare user friend count
	//Result:
	//  - Table Registered: user_friend_count
	//  - Table Schema: user_id, friend_count
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def countUserFriends(sc:SQLContext):DataFrame = {
		//user friend data-frame
		val user_friend_count = sc.sql("SELECT user_id, COUNT(friend_id) AS friend_count FROM user_friend GROUP BY user_id")
		//register temp table
		user_friend_count.registerTempTable("user_friend_count")
		//return
		user_friend_count
	}
	
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //prepare user friend
	//Result:
	//  - Table Registered: user_friend
	//  - Table Schema: user_id, friend_id
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def prepareUserFriend(sc:SQLContext, ufRows:RDD[UserFriendRow], ufSchema:StructType):DataFrame = {
	    //import
        import sc.implicits._

		//user friend data-frame
		val user_friend = sc.createDataFrame(ufRows.map(t => org.apache.spark.sql.Row(t.UserID, t.FriendID)), ufSchema)
		//register temp table
		user_friend.registerTempTable("user_friend")
		//return
		user_friend
    }

	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //prepare user-event
	//Result:
	//  - Table Registered: user_event 
	//  - Table Schema: user_id, event_id, user_invited, view_ahead_days, event_creator, user_interested
	//--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    def prepareUserEvent(sc:SQLContext, ueRows:RDD[UserEventRow], ueSchema:StructType, eventRows:RDD[EventRow], eventSchema:StructType):DataFrame = {
	    //import
        import sc.implicits._

        //user-event rows
        val ues = ueRows.map(t => org.apache.spark.sql.Row(t.UserID, t.EventID, t.Invited, t.TimeStamp, t.Interested, t.TimeStamp.matches(UserEvent.TimeStampFormat)))
        //data-frame
        var dfUserEvent = sc.createDataFrame(ues, ueSchema)
        //register train table 
        dfUserEvent.registerTempTable("temp_user_event")

        //event rows
        val events = eventRows.map(e => org.apache.spark.sql.Row(e.EventID, e.StartTime, e.City, e.State, e.Zip, e.Country, e.Latitude, e.Longitude, e.UserID, e.StartTime.matches(Event.StartTimeFormat)))
        //event data-frame
        var dfEvent = sc.createDataFrame(events, eventSchema)
        //register event table
        dfEvent.registerTempTable("event")

        //join
        var dfJoin = sc.sql("SELECT ue.user, ue.event, ue.invited, ue.time_stamp, ue.interested, ue.valid AS ue_valid, e.user_id AS event_creator, e.start_time, e.valid AS event_valid FROM temp_user_event ue INNER JOIN event e ON ue.event = e.event_id")
        //function
        val daysFun: ((String, String) => Int) = (dts1:String, dts2:String) => Util.getDays(dts1, dts2)
        //filter
        var dfValid = dfJoin.filter($"ue_valid" && $"event_valid")
              .withColumn("user_id", col("user"))
              .withColumn("event_id", col("event"))
              .withColumn("user_invited", col("invited"))
              .withColumn("view_ahead_days", callUDF(daysFun, IntegerType, col("time_stamp"), col("start_time")))
              .withColumn("user_interested", col("interested"))
            .registerTempTable("user_event")
        //create the user-event
        sc.sql("SELECT user_id, event_id, user_invited, view_ahead_days, event_creator, user_interested FROM user_event")       
    }
}
