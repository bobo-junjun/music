package com.spark_music.ok.song

/**
 * 从这个to_song_info_d    hive表中读取数据做清洗
 * 然后插入这个这个hive表   table tw_song_baseinfo_d
 */

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON
import com.spark_music.ok.song.ProduceClientLog.{HDFS_PATH, IF_LOCAL, musicRDD, sc, sparkSession}
import com.spark_music.ok.utils.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object GenerateTwSongBaseinfoD {
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
    } else {
      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
    }

    /**
     * 注册一个udf函数  把专辑名称取出来 没有的返回暂无专辑
     */
    sparkSession.udf.register("getAlbum", (album: String) => {
      var albumName = "暂无专辑"
      try {
        val pattern = Pattern.compile(".*?《(.*?)》.*?")
        val matcher = pattern.matcher(album)
        if (matcher.find()) {
          albumName = matcher.group(1)
        }
      } catch {
        case exception: Exception =>
          albumName
      }
      albumName
    })




    /**
     * 定义udf函数，将科学计数法解析为yyyy-MM-dd HH:mm:ss这种格式，不符合要求的返回 ""
     */
    sparkSession.udf.register("getPostTime", (times: String) => {
      var postTime = ""
      try {
        val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        if (times.contains("E")) {
          postTime = dataFormat.format(times.toDouble)
        } else {
          //dataFormat.parse(times)解析文本返回Date类型
          postTime = dataFormat.format(dataFormat.parse(times))
        }
      } catch {
        case exception: Exception => postTime
      }
      postTime
    })


    /**
     * 注册一个udf函数 实现功能：获取演员信息列的  演员1的name和  演员1的id
     * 演员2的name和  演员2的id  一列拆分为四个字段  通过传参确定需要哪个字符
     */
    sparkSession.udf.register("getSingerAndID", (singerInfo: String, singerOrId: String) => {
      var singerName1 = ""
      var singerId1 = ""
      var singerName2 = ""
      var singerId2 = ""
      var a: String = ""
      try {
        val jSONArray = JSON.parseArray(singerInfo)
        if (jSONArray.size() > 0 && singerOrId.equals("singerName1")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singerName1 = nObject.getString("name")
          a = singerName1
        } else if (jSONArray.size() > 0 && singerOrId.equals("singerId1")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singerId1 = nObject.getString("id")
          a = singerId1
        } else if (jSONArray.size() > 1 && singerOrId.equals("singerName2")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singerName2 = nObject.getString("name")
          a = singerName2
        } else if (jSONArray.size() > 1 && singerOrId.equals("singerId2")) {
          val nObject = JSON.parseObject(jSONArray.get(0).toString)
          singerId2 = nObject.getString("id")
          a = singerId2
        }

      } catch {
        case exception: Exception => a = null
      }
      a
    }
    )

    /**
     * 注册一个udf函数，productType这个字段的double转Int类型
     */
    sparkSession.udf.register("getProductTypeInfo", (productType: String) => {
      val ints = new ListBuffer[Int]

      // 正则匹配吧[] 去掉  然后根据 ","分隔 返回一个数组
      try {
        val strings = productType.replaceAll("[\\[\\]]", "")
          .split(",")
        for (i <- strings) {
          ints.append(i.toDouble.toInt)
        }
      } catch {
        case exception: Exception => null
      }
      ints
    }
    )

    /**
     * 查询我们需要的字段插入  里面使用我们自定义的函数
     */
    sparkSession.sql(s"use ${HIVE_DATABASE_NAME}")
    sparkSession.sql(
      """
        |insert overwrite table tw_song_baseinfo_d
        |select
        |nbr,   --id
        |nvl(name,other_name), --名字如果name为null就替换为other_name
        |source,  --来源
        |getAlbum(album),  --所属专辑
        |prdct,  --发行公司
        |lang,    --歌曲语言
        |video_format,
        |dur,
        |getSingerAndId(singer_info,"singerName1"),
        |getSingerAndId(singer_info,"singerName2"),
        |getSingerAndId(singer_info,"singerId1"),
        |getSingerAndId(singer_info,"singerId2"),
        |0,
        |getPostTime(post_time),
        |pinyin_fst,
        |pinyin,
        |sing_type,
        |ori_singer,
        |lyricist,
        |composer,
        |bpm_val,
        |star_level,
        |video_qlty,
        |video_mk,
        |video_ftur,
        |lyric_ftur,
        |img_qlty,
        |subtitles_type,
        |audio_fmt,
        |ori_sound_qlty,
        |ori_trk,
        |ori_trk_vol,
        |acc_ver,
        |acc_qlty,
        |acc_trk_vol,
        |acc_trk,
        |width,
        |height,
        |video_rsvl,
        |song_ver,
        |auth_co,
        |state,
        |getProductTypeInfo(prdct_type) from to_song_info_d
        |""".stripMargin
    )
    //        .show(false)


  }
}
