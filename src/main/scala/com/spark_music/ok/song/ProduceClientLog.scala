package com.spark_music.ok.song

/**
 * 读取tar包解析数据
 * 将需要的数据插入hive表to_minik_client_song_play_operate_req_d
 */

import com.alibaba.fastjson.JSON
import com.spark_music.ok.base.ParRDDMultipleTextOutputFormat
import com.spark_music.ok.utils.ConfigUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkContext}

object ProduceClientLog {
  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  var musicRDD: RDD[String] = _
  val HIVE_DATABASE_NAME: String = ConfigUtils.HIVE_DATABASE_NAME
  val IF_LOCAL: Boolean = ConfigUtils.IF_LOCAL
  val HDFS_PATH: String = ConfigUtils.HDFS_PATH

  def main(args: Array[String]): Unit = {

    //    由于此类 每天至少运行一次，数据基于天 进行分区，所以 必须传入一个 时间参数
    if (args.length < 1) {
      println("需要传入 时间参数")
      System.exit(1)
    }
    if (IF_LOCAL) {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      musicRDD = sc.textFile("file:///F:\\ideaProject3-2019\\myspark\\music\\data\\currentday_clientlog.tar.gz")
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      //      musicRDD = sc.textFile("file:///D:\\IdeaProject\\MusicProject\\datas\\currentday_clientlog.tar.gz")
      musicRDD = sc.textFile(s"${HDFS_PATH}${args(0)}/currentday_clientlog.tar.gz")
    }

    val cleanRDD: RDD[(String, String)] = musicRDD.map(_.split("&"))
      .filter(_.length == 6) // 过滤脏数据
      .map(arr => (arr(2), arr(3)))
    //    获取有多少种标识,  获取key值，去重，收集到数组中，获取数组的长度
    val dataTypeNum = cleanRDD.keys.distinct().collect().length
    //    println(dataTypeNum)
    //    cleanRDD.foreach(println)
    cleanRDD
      //      将数据分别以标识名的方式存储在hdfs某个路径中， 也就是说我们需要通过 标识来进行分区，不同的数据写入不同文件
      .partitionBy(new HashPartitioner(dataTypeNum))
      //      由于saveAsTextFile，不能够自定义文件名字，所以 替换掉
      //      .saveAsTextFile("/logdata/all_client_tables")
      .saveAsHadoopFile(s"${HDFS_PATH}all_client_tables/${args(0)}", classOf[String], classOf[String], classOf[ParRDDMultipleTextOutputFormat])


    sc.textFile(s"${HDFS_PATH}all_client_tables/${args(0)}/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ").map(line => {
      //        根据分隔符分割，获取需要的数据
      val strings = line.split("\t")
      val musicData = strings(1)
      //        由于数据格式是 json格式，所以将匹配的数据放入json中，进行处理
      val nObject = JSON.parseObject(musicData)
      //        因为是将数据写入 TO_MINIK_CLIENT_SONG_PLAY_OPERATE_REQ_D，所以需要查看此表需要的字段
      val songid = nObject.getString("songid")
      val mid = nObject.getString("mid")
      val optrate_type = nObject.getString("optrate_type")
      val uid = nObject.getString("uid")
      val consume_type = nObject.getString("consume_type")
      val dur_time = nObject.getString("dur_time")
      val session_id = nObject.getString("session_id")
      val songname = nObject.getString("songname")
      val pkg_id = nObject.getString("pkg_id")
      val order_id = nObject.getString("order_id")
      //        将得到的数据字段，放入元祖中，元祖中有两个元素，key是 数据标识，value中是 标识对应的数据。，字段以\t分割
      (songid + "\t" + mid + "\t" + optrate_type + "\t" + uid + "\t" + consume_type + "\t" + dur_time + "\t" + session_id + "\t" + songname + "\t" + pkg_id + "\t" + order_id)
    }).saveAsTextFile(s"/user/hive/warehouse/data/song/TO_MINIK_CLIENT_SONG_PLAY_OPERATE_REQ_D/data_dt=${args(0)}")
    sparkSession.sql(s"MSCK REPAIR TABLE ${HIVE_DATABASE_NAME}.to_minik_client_song_play_operate_req_d")


  }
}

