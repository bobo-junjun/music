package com.spark_music.my

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ProduceClientLog {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("需要传入时间参数")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("CleanData")
      .master("local[6]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sourceRdd = sc.textFile("hdfs://192.168.8.105:8020/mycluster/logdata/currentday_clientlog.tar.gz")
    //        sourceRdd.foreach(println)
    //    1575336797&96075&MINIK_CLIENT_ADVERTISEMENT_RECORD&{"src_verison": 2546, "mid": 96075, "adv_type": 4, "src_type": 2546, "uid": 0,
    //    "session_id": 56738, "event_id": 1, "time": 1575336796}&3.0.1.15&2.4.4.30
    val value: RDD[(String, String)] = sourceRdd.map(_.split("&"))

      .filter(arr => arr.length == 6)
      //      .filter(arr => ("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ").equals(arr(2)))
      .map { x =>
        (x(2), x(3)) // x(2)是唯一标识 x(3)是Json格式
      }
    val tablesNameNums: Int = value.map(_._1).distinct().collect().length

    //      将所有文件按  tableName 分区 写入 不同的文件中  文件名为 tableName
    /*   value.partitionBy(new HashPartitioner(tablesNameNums))
         //默认文件的名字是part0000   所以我们要自定义名字
         .saveAsHadoopFile(s"hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables/${args(0)}"
           , classOf[String]
           , classOf[String]
           , classOf[DefineFileName]
         )*/

    //将我们要的数据的用spark sql 写入hive表中
    //    spark.sql(s"use music")
    //    spark.sql(
    //      s"""
    //         |load data inpath "hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ"
    //         |overwrite into table to_minik_client_song_play_operate_req_d partiton(data_dt=${args(0)})
    //         |""".stripMargin
    //    )
    val rdd1: RDD[String] = spark.sparkContext.textFile(s"hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables/${args(0)}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")

    val json: RDD[String] = rdd1.map(_.split("\t"))
      .map(_ (1)) //取出json字段

    //      json.foreach(println)
    val frame = json.map { x =>

      val jSONObject = JSON.parseObject(x) //解析JSon字段插入hive表
      //        println(x._2)   //打印看下一结果
      val songid = jSONObject.getString("songid")
      val mid = jSONObject.getString("mid")
      val optrate_type = jSONObject.getString("optrate_type")
      val uid = jSONObject.getString("uid")
      val consume_type = jSONObject.getString("consume_type")
      val dur_time = jSONObject.getString("dur_time")
      val session_id = jSONObject.getString("session_id")
      val songname = jSONObject.getString("songname")
      val pkg_id = jSONObject.getString("pkg_id")
      val order_id = jSONObject.getString("order_id")
      //        返回一个元组(tableName,解析JSon后的数据)
      JsonFilds(songid, mid, optrate_type, uid, consume_type, dur_time, session_id, songname, pkg_id, order_id)
      //        ( songid + "\t" + mid + "\t" + optrate_type + "\t" + uid + "\t" + consume_type + "\t" + dur_time + "\t" + session_id + "\t" + songname + "\t" + pkg_id + "\t" + order_id)
    }.toDF()
    frame.createTempView("Tmp")
    spark.sql("select * from Tmp").write
      .mode("overwrite")
      .partitionBy(args(0))
      .saveAsTable("music.to_minik_client_song_play_operate_req_d")

  }
}

case class JsonFilds(songid: String, mid: String, optrate_type: String, uid: String, consume_type: String, dur_time: String, session_id: String, songname: String, pkg_id: String, order_id: String)
