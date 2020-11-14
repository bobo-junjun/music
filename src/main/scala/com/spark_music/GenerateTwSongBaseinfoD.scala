package com.spark_music

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GenerateTwSongBaseinfoD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[3]").appName("GenerateTwSongBaseinfoD")
      .config("hadoop.home.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    //    spark.sql("show databases").show
    spark.sql("use music")
    //    spark.sql("show tables").show
    val sourceDF: DataFrame = spark.sql("select * from  to_song_info_d limit 10")
    sourceDF.map{
      row:Row=>{
        row.getInt(1)
      }
    }.show()
//      sourceDF.createTempView("temp")
//    //    spark.sql("select ablum from  to_song_info_d limit 10 ").show()
//    val Ds: Dataset[Song] = sourceDF.as[Song]
//    Ds.rdd.map {
//       song: Song =>
//        song.ALBUM.replaceAll(null,"暂无数据")
//    }
//      .foreach(println)


  }
}
case   class Song(NBR: String,NAME: String,OTHER_NAME: String,SOURCE: String,ALBUM: String,
                  PRDCT: String,LANG: String,VIDEO_FORMAT: String,DUR: String,
                  SINGER_INFO: String,POST_TIME: String,PINYIN_FST: String,PINYIN: String,
                  SING_TYPE: String,ORI_SINGER: String,LYRICIST: String,COMPOSER: String,
                  BPM_VAL: String,STAR_LEVEL: String,VIDEO_QLTY: String,VIDEO_MK: String,
                  VIDEO_FTUR: String,LYRIC_FTUR: String,IMG_QLTY: String,SUBTITLES_TYPE: String,
                  AUDIO_FMT: String,ORI_SOUND_QLTY: String,ORI_TRK: String,ORI_TRK_VOL: String,
                  ACC_VER: String,ACC_QLTY: String,ACC_TRK_VOL: String,ACC_TRK: String,WIDTH: String,
                  HEIGHT: String,VIDEO_RSVL: String,SONG_VER: String,AUTH_CO: String,STATE: String,
                  PRDCT_TYPE: String)
