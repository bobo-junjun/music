package com.spark_music.my

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object GenerateTwSongBaseinfoD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[3]").appName("GenerateTwSongBaseinfoD")
      .config("hadoop.home.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    //    spark.sql("show databases").show
    spark.sql("use music")
    //    spark.sql("show tables").show
    val sourceDF: DataFrame = spark.sql("select * from  to_song_info_d limit 100")

    //      sourceDF.createTempView("temp")
    //    //    spark.sql("select ablum from  to_song_info_d limit 10 ").show()
    val Ds: Dataset[Song] = sourceDF.as[Song]
    //   sourceDF.na.fill(Map(
    //     "name" -> "othername"
    //   )

    //   )
    //    val rdd = Ds.rdd
    //    rdd.foreach(println)

    //      sourceDF.rdd.foreach(println)


    val rdd: RDD[Song] = Ds.rdd
      //  1.  获取专辑 ALBUM 名称，没有专辑返回 暂无专辑
      .map {
        var c5 = ""
        song: Song =>
          c5 = song.ALBUM //alnum
          try {
            //如果是[json] 格式用JSON.parseArray之后再用parseObject解析
            if (c5.contains("[") || c5.contains("]")) {
              val array = JSON.parseArray(c5)
              val value = array.get(0)
              val nObject = JSON.parseObject(value.toString)
              c5 = nObject.getString("name")
            } else {
              //如果是json 格式用parseObject解析
              val nObject = JSON.parseObject(c5)
              c5 = nObject.getString("name")
            }
          } catch {
            case exception: Exception => c5 = "暂无专辑"
          }

          //          try {
          //            val nObject = JSON.parseObject(c5)
          //            c5 = nObject.getString("name")
          //          } catch {
          //            case exception: Exception => c5 = "暂无专辑"
          //          }
          song.copy(ALBUM = c5)
      }
      //测试album列
      //      .map{song: Song=>
      //        song.ALBUM
      //      }

      //   2.   时间进行格式化 POST_TIME  yyyy-MM-dd HH:mm:ss ，没有时间返回 null
      .map {
        song: Song =>
          var c10 = ""
          c10 = song.POST_TIME
          val pattern = "^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d$"
          val r = Pattern.compile(pattern)
          val m = r.matcher(c10)

          try {
            if (!m.matches) {
              c10 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c10.toDouble)
            }
          }
          catch {
            case exception: Exception => c10 = null
          }

          song.copy(POST_TIME = c10)

      }
      //      打印时间列看
      //      .map{song: Song=>
      //                song.POST_TIME
      //      }


      //  3.  获取 SINGER_INFO 中的singer和singerid
      .map { song: Song =>
        var c9 = song.SINGER_INFO
        var c9Name1 = ""
        var c9Name2 = ""
        var c9Id1 = ""
        var c9Id2 = ""
        try {
          //如果是[json] 格式用JSON.parseArray之后再用parseObject解析
          if (c9.contains("[") || c9.contains("]")) {
            val array = JSON.parseArray(c9)
            val value = array.get(0)
            val nObject = JSON.parseObject(value.toString)
            c9Name1 = nObject.getString("name")
            c9Id1 = nObject.getString("id")
            c9Name2 = nObject.getString("name")
            c9Id2 = nObject.getString("id")
            c9 = c9Name1 + "&" + c9Id1 + "&" + c9Name2 + "&" + c9Id2
          } else {
            //如果是json 格式用parseObject解析
            val nObject = JSON.parseObject(c9)
            c9Name1 = nObject.getString("name")
            c9Id1 = nObject.getString("id")
            c9Name2 = nObject.getString("name")
            c9Id2 = nObject.getString("id")
            c9 = c9Name1 + "&" + c9Id1 + "&" + c9Name2 + "&" + c9Id2

          }
        } catch {
          case exception: Exception => c9 = "null "+ "&" + "null" + "&" + "null" + "&" + "null"
        }
        song.copy(SINGER_INFO = c9)
      }
      //        打印singer_info名字和id看一下
      //      .map {
      //        song: Song =>
      //          song.SINGER_INFO
      //      }

      // 4.   判断 to_song_info_d.name是否为null，为null替换为 to_song_info_d.othername

      //      .map { song: Song =>
      //        var c2 = song.NAME
      //        if (c2 == null || c2 == "") {
      //          c2 = song.OTHER_NAME
      //        }
      //        song.copy(NAME = c2)
      //      }


      //  打印name列看一下
      //            .map{
      //              song : Song =>
      //                 song.NAME
      //            }

      //    //   5. 将 productTypeInfo 中的数字转为int类型，假如productTypeInfo的长度为0，返回null
      .map { song: Song =>
        var c40 = song.PRDCT_TYPE
        try {
          if (c40.length == 0 || c40 == null) {
            c40 = "0"
          } else {
            val str1 = c40.substring(1, c40.length - 1)
            val strings: Array[String] = str1.split(",")
            if (strings.length == 2) {
              val int1 = strings(0).toDouble.toInt
              val int2 = strings(1).toDouble.toInt
              c40 = int1 + "," + int2
            } else if (strings.length == 1) {
              val int1 = strings(0).toDouble.toInt
              c40 = int1.toString
            } else if (strings.length == 3) {
              val int1 = strings(0).toDouble.toInt
              val int2 = strings(1).toDouble.toInt
              val int3 = strings(2).toDouble.toInt
              c40 = int1 + "," + int2 + "," + int3
            }
          }
        } catch {
          case exception: Exception => c40 = "0"
        }
        song.copy(PRDCT_TYPE = c40)
      }

    //      打印PRDCT_TYPE看看效果
    //      .map { song: Song =>
    //        song.PRDCT_TYPE
    //      }
      .map{song: Song =>
        var songver = song.SONG_VER
        if(songver == null || songver == "" || songver.isEmpty){
          songver = "0"
        }
        song.copy(SONG_VER = songver)
      }



      .map{song: Song =>
          var auth = song.AUTH_CO
        try {
          if (auth == null || auth == "" || auth.isEmpty || auth.contains("{")) {
            auth = "0"
          }
        } catch {
          case exception: Exception => auth = "0"
        }
        song.copy(AUTH_CO = auth)
      }
//        rdd.map { song: Song =>
//          song.SONG_VER
//        }
//
//         .foreach(println)


    val value: Dataset[BaseInfo] = rdd.map { song: Song =>
      BaseInfo(song.NBR, song.NAME, song.SOURCE.toInt, song.ALBUM, song.PRDCT, song.LANG, song.VIDEO_FORMAT, song.DUR.toInt,
        song.SINGER_INFO.split("&")(0),
        song.SINGER_INFO.split("&")(1),
        song.SINGER_INFO.split("&")(2),
        song.SINGER_INFO.split("&")(3),
        0,
        song.POST_TIME, song.PINYIN_FST, song.PINYIN, song.SING_TYPE.toInt, song.ORI_SINGER,
        song.LYRICIST, song.COMPOSER, song.BPM_VAL.toInt, song.STAR_LEVEL.toInt, song.VIDEO_QLTY.toInt,
        song.VIDEO_MK.toInt, song.VIDEO_FTUR.toInt, song.LYRIC_FTUR.toInt, song.IMG_QLTY.toInt, song.SUBTITLES_TYPE.toInt,
        song.AUDIO_FMT.toInt, song.ORI_SOUND_QLTY.toInt, song.ORI_TRK.toInt, song.ORI_TRK_VOL.toInt, song.ACC_VER.toInt,
        song.ACC_QLTY.toInt, song.ACC_TRK_VOL.toInt, song.ACC_TRK.toInt, song.WIDTH.toInt, song.HEIGHT.toInt, song.VIDEO_RSVL.toInt,
        song.SONG_VER.toInt,
        song.AUTH_CO.toInt,
        song.STATE.toInt, song.PRDCT_TYPE.split(",").map(x=>x.toDouble.toInt))
    }.toDS()


      value.createTempView("Tmp")
        spark.sql("select * from Tmp")
      .write.mode("overwrite")
      .saveAsTable("TW_SONG_BASEINFO_D")
  }


}

case class Song(NBR: String, NAME: String, OTHER_NAME: String, SOURCE: String, ALBUM: String,
                PRDCT: String, LANG: String, VIDEO_FORMAT: String, DUR: String,
                SINGER_INFO: String, POST_TIME: String, PINYIN_FST: String, PINYIN: String,
                SING_TYPE: String, ORI_SINGER: String, LYRICIST: String, COMPOSER: String,
                BPM_VAL: String, STAR_LEVEL: String, VIDEO_QLTY: String, VIDEO_MK: String,
                VIDEO_FTUR: String, LYRIC_FTUR: String, IMG_QLTY: String, SUBTITLES_TYPE: String,
                AUDIO_FMT: String, ORI_SOUND_QLTY: String, ORI_TRK: String, ORI_TRK_VOL: String,
                ACC_VER: String, ACC_QLTY: String, ACC_TRK_VOL: String, ACC_TRK: String, WIDTH: String,
                HEIGHT: String, VIDEO_RSVL: String, SONG_VER: String, AUTH_CO: String, STATE: String,
                PRDCT_TYPE: String)

case class BaseInfo(NBR: String, NAME: String, SOURCE: Int, ALBUM: String, PRDCT: String, LANG: String,
                    VIDEO_FORMAT: String, DUR: Int, SINGER1: String, SINGER2: String, SINGER1ID: String,
                    SINGER2ID: String, MAC_TIME: Int, POST_TIME: String, PINYIN_FST: String, PINYIN: String,
                    SING_TYPE: Int, ORI_SINGER: String, LYRICIST: String, COMPOSER: String, BPM_VAL: Int,
                    STAR_LEVEL: Int, VIDEO_QLTY: Int, VIDEO_MK: Int, VIDEO_FTUR: Int, LYRIC_FTUR: Int,
                    IMG_QLTY: Int, SUBTITLES_TYPE: Int, AUDIO_FMT: Int, ORI_SOUND_QLTY: Int, ORI_TRK: Int,
                    ORI_TRK_VOL: Int, ACC_VER: Int, ACC_QLTY: Int, ACC_TRK_VOL: Int, ACC_TRK: Int,
                    WIDTH: Int, HEIGHT: Int, VIDEO_RSVL: Int, SONG_VER: Int, AUTH_CO: Int,
                    STATE: Int, PRDCT_TYPE: Array[Int])

//测试类
case class BaseInfo1(NBR: String, NAME: String)