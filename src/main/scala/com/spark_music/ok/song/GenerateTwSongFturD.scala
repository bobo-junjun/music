package com.spark_music.ok.song

/**
 * 将几个几天的表关联 起来做分析
 */

import com.spark_music.ok.song.GenerateTwSongBaseinfoD.{IF_LOCAL, sc, sparkSession}
import com.spark_music.ok.utils.{ConfigUtils, MyDataUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GenerateTwSongFturD {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  var musicRDD: RDD[String] = _
  val HIVE_DATABASE_NAME: String = ConfigUtils.HIVE_DATABASE_NAME
  val IF_LOCAL: Boolean = ConfigUtils.IF_LOCAL
  val HDFS_PATH: String = ConfigUtils.HDFS_PATH

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
    //获取一周前的时间
    val date7Before = MyDataUtils.getDataTime(s"${args(0)}", 7)
    //获取30天前的时间
    val date30Before = MyDataUtils.getDataTime(s"${args(0)}", 30)
    sparkSession.sql(s"use ${HIVE_DATABASE_NAME}")
    /**
     * 查询to_minik_client_song_play_operate_req_d这个表
     * 统计   当天的点唱数..
     *
     */
    val sing_sameDay = sparkSession.sql(
      s"""
         |select
         |songid,
         |count(songid) as sing_cnt,      --当日点唱数
         |0 as supp_cnt,                  --点赞数（没有统计），默认为零
         |count(distinct uid) as usr_cnt,
         |count(distinct order_id) as ordr_cnt
         |from  to_minik_client_song_play_operate_req_d
         |where data_dt=${args(0)}
         |group by songid
         |""".stripMargin)
//    sing_sameDay.show(false)


    /**
     * 查询to_minik_client_song_play_operate_req_d这个表中
     * 统计出    7天的点唱数..
     */
    val rct_7_sing: DataFrame = sparkSession.sql(
      s"""
         |SELECT songid,
         |      count(songid) AS rct_7_sing_cnt,
         |      0 as rct_7_supp_cnt,
         |      count(DISTINCT UID) AS rct_7_usr_cnt,
         |      count(DISTINCT order_id) AS rct_7_ordr_cnt
         |FROM to_minik_client_song_play_operate_req_d
         |WHERE data_dt between ${date7Before} and ${args(0)}    --条件是这七天
         |GROUP BY songid
         |""".stripMargin)
//    rct_7_sing.show(false)


    /**
     * 查询to_minik_client_song_play_operate_req_d这个表中
     * *统计出    30天的点唱数 ...
     */
    val rct_30_sing: DataFrame = sparkSession.sql(
      s"""
         |SELECT songid,
         |      count(songid) AS rct_30_sing_cnt,
         |      0 as rct_30_supp_cnt,
         |      count(DISTINCT UID) AS rct_30_usr_cnt,
         |      count(DISTINCT order_id) AS rct_30_ordr_cnt
         |FROM to_minik_client_song_play_operate_req_d
         |WHERE data_dt between ${date30Before} and ${args(0)}  --条件是这30天
         |GROUP BY songid
         |""".stripMargin)


    /**
     * 查询tw_song_ftur_d这个表
     * 统计出  7 天和 30 天 最高的点唱数...
     * tw_song_ftur_d这个表不存在是因为这是第一天，我们统计七天的，往后就有了
     */
    val rct_7_30_top: DataFrame = sparkSession.sql(
      s"""
         |select
         |nbr,
         |max(case when data_dt>=${date7Before} and data_dt<=${args(0)} then sing_cnt end) rct_7_top_sing_cnt,
         |max(case when data_dt>=${date7Before} and data_dt<=${args(0)} then supp_cnt end) rct_7_top_supp_cnt,
         |max(sing_cnt) rct_30_top_sing_cnt,
         |max(supp_cnt) rct_30_top_supp_cnt from tw_song_ftur_d where data_dt between ${date30Before} and ${args(0)}
         |group by nbr
         |""".stripMargin)




    //将上述4个ds注册成临时视图
    sing_sameDay.createOrReplaceTempView("sing_sameDay")
    rct_7_sing.createOrReplaceTempView("rct_7_sing")
    rct_30_sing.createOrReplaceTempView("rct_30_sing")
    rct_7_30_top.createOrReplaceTempView("rct_7_30_top")

    /**
     * 左关联上述四个表，查询相应的字段插入tw_song_ftur_d中
     */
    sparkSession.sql(
      s"""
         |insert overwrite table tw_song_ftur_d partition(data_dt=${args(0)})
         |select
         |a.NBR,
         |a.name,
         |SOURCE,
         |ALBUM,
         |PRDCT,
         |LANG,
         |VIDEO_FORMAT,
         |DUR,
         |SINGER1,
         |SINGER2,
         |SINGER1ID,
         |SINGER2ID,
         |MAC_TIME,
         |nvl(SING_CNT,0),      --如果为null，则替换成0
         |nvl(SUPP_CNT,0),      --如果为null，则替换成0
         |nvl(USR_CNT,0),       --如果为null，则替换成0
         |nvl(ORDR_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_7_SING_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_7_SUPP_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_7_TOP_SING_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_7_TOP_SUPP_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_7_USR_CNT,0),       --如果为null，则替换成0
         |nvl(RCT_7_ORDR_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_30_SING_CNT,0),       --如果为null，则替换成0
         |nvl(RCT_30_SUPP_CNT,0),       --如果为null，则替换成0
         |nvl(RCT_30_TOP_SING_CNT,0),       --如果为null，则替换成0
         |nvl(RCT_30_TOP_SUPP_CNT,0),       --如果为null，则替换成0
         |nvl(RCT_30_USR_CNT,0),      --如果为null，则替换成0
         |nvl(RCT_30_ORDR_CNT,0)
         |from tw_song_baseinfo_d a
         |left join sing_sameDay b on a.nbr=b.songid
         |left join rct_7_sing c on c.songid = b.songid
         |left join rct_30_sing d on d.songid = c.songid
         |left join rct_7_30_top e on e.nbr = d.songid
         |""".stripMargin)
//      .show(false)
  }
}
