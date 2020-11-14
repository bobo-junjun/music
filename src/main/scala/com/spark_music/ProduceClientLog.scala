package com.spark_music

import com.alibaba.fastjson.JSON
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSONObject

object ProduceClientLog {
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      println("需要传入时间参数")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("CleanData")
      .master("local[6]").getOrCreate()
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
    value.map { x =>
      //如果是MINIK_CLIENT_SONG_PLAY_OPERATE_REQ  我们写入hive表
      if (("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ").equals(x._2)) {
        val tableName = x._1
        val jSONObject = JSON.parseObject(x._2) //解析JSon字段插入hive表
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
        (tableName, songid + "\t" + mid + "\t" + optrate_type + "\t" + uid + "\t" + consume_type + "\t" + dur_time
          + "\t" + session_id + "\t" + songname + "\t" + pkg_id + "\t" + order_id)
      } else {
        //      把所有数据返回
        x
      }
    }
      //      将所有文件按  tableName 分区 写入 不同的文件中  文件名为 tableName
      .partitionBy(new HashPartitioner(tablesNameNums))
      //默认文件的名字是part0000   所以我们要自定义名字
      .saveAsHadoopFile("hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables"
        , classOf[String]
        , classOf[String]
        , classOf[DefineFileName]
      )

    //将我们要的数据的用spark sql 写入hive表中
    spark.sql(s"use music")
    spark.sql(
      s"""
         |load data inpath "hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ"
         |overwrite into table to_minik_client_song_play_operate_req_d partiton(data_dt=${args(0)})
         |""".stripMargin
    )


    //      .saveAsTextFile("hdfs://192.168.8.105:8020/mycluster/logdata/all_client_tables/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")
    //      .foreach(println)
  }
}
