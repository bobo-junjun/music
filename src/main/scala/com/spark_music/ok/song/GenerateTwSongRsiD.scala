package com.spark_music.ok.song

/**
 *第一个最终需求：
 * 歌曲热度与歌手热度排行
 */

import java.util.Properties

import com.spark_music.ok.song.GenerateTwSongFturD.{IF_LOCAL, sc, sparkSession}
import com.spark_music.ok.utils.{ConfigUtils, MyDataUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object GenerateTwSongRsiD {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  var musicRDD: RDD[String] = _
  val HIVE_DATABASE_NAME: String = ConfigUtils.HIVE_DATABASE_NAME
  val IF_LOCAL: Boolean = ConfigUtils.IF_LOCAL
  val HDFS_PATH: String = ConfigUtils.HDFS_PATH
  val MYSQL_USERNAME: String = ConfigUtils.MYSQL_USERNAME
  val MYSQL_PASSWORD: String = ConfigUtils.MYSQL_PASSWORD
  val MYSQL_JDBC_URL: String = ConfigUtils.MYSQL_JDBC_URL

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("需要传入 时间参数")
      System.exit(1)
    }
    if (IF_LOCAL) {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    }

    /**
     * 获取歌曲的热度（根据公式）
     * totalSing    歌曲的总点唱数
     * day          天
     * totalSupport 歌曲总点赞数
     * topSingHits  最高点唱数
     * topSupport   最高点赞数
     * RSI 歌曲的热度
     */
    val getRsi: (Int, Int, Int, Int, Int) => String = (totalSing: Int, totalSupport: Int, day: Int, topSingHits, topSupport: Int) => {
      if (day == 1) {
        (Math.pow(Math.log(totalSing / day + 1) * 0.63 * 0.8 + Math.log(topSupport / day + 1) * 0.63 * 0.2, 2) * 10).toString
      }
      (Math.pow(
        (Math.log(totalSing / day + 1) * 0.63 + 0.37 * Math.log(topSingHits / day + 1)) * 0.8 +
          (Math.log(totalSupport / day + 1) * 0.63 + 0.37 * Math.log(topSupport / day + 1)) * 0.2
        , 2) * 10).toString
    }

//注册成udf函数
    sparkSession.udf.register("getRsi", getRsi)

    //获取前七天   和前三十天
    val per7 = MyDataUtils.getDataTime(args(0), 7)
    val per30 = MyDataUtils.getDataTime(args(0), 30)


    //    从 TW_SONG_FTUR_D 表中，获取 我们需要的字段
    sparkSession.sql(s"use ${HIVE_DATABASE_NAME}")
    sparkSession.table("TW_SONG_FTUR_D").select(
      "NBR",
      "NAME",
      "SING_CNT",
      "SUPP_CNT",
      "RCT_7_SING_CNT",
      "RCT_7_SUPP_CNT",
      "RCT_7_TOP_SING_CNT",
      "RCT_7_TOP_SUPP_CNT",
      "RCT_30_SING_CNT",
      "RCT_30_SUPP_CNT",
      "RCT_30_TOP_SING_CNT",
      "RCT_30_TOP_SUPP_CNT",
      "DATA_DT"
    )
      .where(s"data_dt>=${per30} and data_dt<=${args(0)}")
      //      .show()
      .createOrReplaceTempView("TW_SONG_FTUR_D_TMP")



    val dataFrame = sparkSession.sql(
      s"""
         |select
         |"1" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt=${args(0)}
         |""".stripMargin)


    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame.createGlobalTempView("TW_SONG_FTUR_D_TMP_1")
    val frame1 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_1
         |""".stripMargin)

    val dataFrame7 = sparkSession.sql(
      s"""
         |select
         |"7" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt>=${per7} and data_dt<=${args(0)}
         |""".stripMargin)


    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame7.createGlobalTempView("TW_SONG_FTUR_D_TMP_7")
    val frame7 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_7
         |""".stripMargin)

    val dataFrame30 = sparkSession.sql(
      s"""
         |select
         |"30" period,
         |nbr,
         |name,
         |getRsi(SING_CNT,SUPP_CNT,1,0,0) rsi
         |from TW_SONG_FTUR_D_TMP where data_dt>=${per30} and data_dt<=${args(0)}
         |""".stripMargin)

    //    创建临时视图后，所有字段变为string，所以想要排序，需要类型转换
    dataFrame30.createGlobalTempView("TW_SONG_FTUR_D_TMP_30")
    val frame30 = sparkSession.sql(
      s"""
         |select
         |period,
         |nbr,
         |name,
         |cast(rsi as double),
         |row_number() over(order by cast(rsi as double) desc) rsi_rank
         |from
         |global_temp.TW_SONG_FTUR_D_TMP_30
         |""".stripMargin)


    //    将当天、最近7天、最近30天的数据进行关联，写入 tm_song_rsi_d 中。(hive表)
    val unionData: Dataset[Row] = frame1.union(frame7).union(frame30)
    unionData.createOrReplaceTempView("result")
sparkSession.sql(
  s"""
     |insert overwrite table tm_song_rsi_d partition(data_dt=${args(0)})
     |select * from result
     |""".stripMargin)

    //    写入mysql中
    val properties = new Properties()
    properties.put("driver", "com.mysql.jdbc.Driver")
    properties.put("user", MYSQL_USERNAME)
    properties.put("password", MYSQL_PASSWORD)
    unionData.write.mode(SaveMode.Overwrite).jdbc(MYSQL_JDBC_URL, "tm_song_rsi", properties)





}
}
