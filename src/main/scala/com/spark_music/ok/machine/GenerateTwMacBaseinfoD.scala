package com.spark_music.ok.machine

import java.util.Properties

import com.spark_music.ok.utils.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object GenerateTwMacBaseinfoD {
//  var sparkSession: SparkSession = _
  var sc: SparkContext = _
  var musicRDD: RDD[String] = _
  val HIVE_DATABASE_NAME2: String = ConfigUtils.HIVE_DATABASE_NAME
  val IF_LOCAL: Boolean = ConfigUtils.IF_LOCAL
  val HDFS_PATH: String = ConfigUtils.HDFS_PATH

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("需要传入 时间参数")
      System.exit(1)
    }
//    if (IF_LOCAL) {
     val sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
//    } else {
//      sparkSession = SparkSession.builder().appName("ProduceClientLog").enableHiveSupport().getOrCreate()
//      sc = sparkSession.sparkContext
      sparkSession.sql(s"use mac")
      val mac_d_mid = sparkSession.table("TO_MNK_MAC_D").select("mid").distinct()
      val admin_map_mid = sparkSession.table("TO_HSKP_MAC_ADMIN_MAP_D").select("mid").distinct()
      //    println(mac_d_mid.union(admin_map_mid).count())
      mac_d_mid.union(admin_map_mid).createOrReplaceTempView("total_mid")

      //    将minik的表进行关联, 指定我们需要的列
      val miniks = sparkSession.sql(
        s"""
           |select
           |a.mid,
           |b.HARD_ID,
           |b.SONG_WHSE_VER,
           |b.EXEC_VER,
           |b.UI_VER,
           |b.CUR_LOGIN_TM,
           |c.PRVC,
           |c.CTY,
           |c.ADDR,
           |c.REV_TM,
           |c.SALE_TM
           |from total_mid a
           |left join TO_MNK_MAC_D b
           |on a.mid=b.mid
           |left join TO_MNK_MAC_LOC_D c
           |on b.mid=c.mid
           |""".stripMargin)

      //    基于 total_mid 表将housekeeper中的六张表进行关联，获取需要的字段
      val housekeeper = sparkSession.sql(
        s"""
           |select
           |a.mid,
           |b.MAC_NM,
           |b.INV_RATE,
           |b.AGE_RATE,
           |b.COM_RATE,
           |b.PAR_RATE,
           |b.PRDCT_TYPE,
           |b.HAD_MPAY_FUNC,
           |b.IS_ACTV,
           |b.ACTV_TM,
           |c.STORE_NM,
           |c.SUB_TAG_NM,
           |c.SUB_SCENE_CATGY_NM,
           |c.BRND_NM,
           |c.SUB_BRND_NM,
           |g.AREA,
           |e.PRVC,
           |f.CTY,
           |c.ADDR
           |from total_mid a
           |left join
           |TO_HSKP_MAC_ADMIN_MAP_D b on a.mid=b.mid
           |left join
           |TW_MAC_BASEINFO_D h on h.mid=b.mid
           |left join
           |TO_HSKP_STORE_D c on c.GROUND_NM=b.GROUND_NM
           |left join
           |TO_HSKP_MAC_STORE_MAP_D d on d.mid=b.mid
           |left join
           |TO_HSKP_PRVC_D e on e.PRVC_ID=c.PRVC_ID
           |left join
           |TO_HSKP_CITY_D f on f.PRVC_ID=e.PRVC_ID
           |left join
           |TO_HSKP_AREA_D g on g.CTY_ID=f.CTY_ID
           |""".stripMargin)
      //      .show(false)
      miniks.createOrReplaceTempView("miniks")
      housekeeper.createOrReplaceTempView("housekeeper")
      //    关联 俩表
      //    miniks.join(housekeeper,Seq("mid"),"left").show(false)
      //      .createOrReplaceTempView("result")

      val result = sparkSession.sql(
        s"""
           |select
           |a.mid	MID	,
           |b.MAC_NM	MAC_NM	,
           |a.SONG_WHSE_VER	SONG_WHSE_VER	,
           |a.EXEC_VER	EXEC_VER	,
           |a.UI_VER	UI_VER	,
           |a.HARD_ID	HARD_ID	,
           |a.SALE_TM	SALE_TM	,
           |a.REV_TM	REV_TM	,
           |"移动"	OPER_NM	,
           |if(a.PRVC==null,b.PRVC,a.PRVC)	PRVC	,
           |if(a.CTY==null,b.CTY,a.CTY)	CTY	,
           |b.AREA	AREA	,
           |if(b.ADDR==null,a.ADDR,b.ADDR)	ADDR	,
           |b.STORE_NM	STORE_NM	,
           |b.SUB_TAG_NM	SCENCE_CATGY	,
           |b.SUB_SCENE_CATGY_NM	SUB_SCENCE_CATGY	,
           |null	SCENE	,
           |null	SUB_SCENE	,
           |b.BRND_NM	BRND	,
           |b.SUB_BRND_NM	SUB_BRND	,
           |null	PRDCT_NM	,
           |b.PRDCT_TYPE	PRDCT_TYP	,
           |"加盟"	BUS_MODE	,
           |b.INV_RATE	INV_RATE	,
           |b.AGE_RATE	AGE_RATE	,
           |b.COM_RATE	COM_RATE	,
           |b.PAR_RATE	PAR_RATE	,
           |b.IS_ACTV	IS_ACTV	,
           |b.ACTV_TM	ACTV_TM	,
           |b.HAD_MPAY_FUNC	PAY_SW	,
           |"houda"	PRTN_NM	,
           |a.CUR_LOGIN_TM	CUR_LOGIN_TM
           |from miniks a
           |join housekeeper b on a.mid=b.mid
           |""".stripMargin)
     import sparkSession.implicits._
      val value: Dataset[String] = result.as[String]
//      value.createOrReplaceTempView("result2")
      //   将结果数据写入hive tw_mac_baseinfo_d的指定分区
      /*sparkSession.sql(
        s"""
           |insert overwrite table tw_mac_baseinfo_d partition(data_dt=${args(0)})
           |select * from result
           |""".stripMargin)*/

      //    写入mysql ,mysql的表会自动创建
      val properties = new Properties()
      properties.put("driver", "com.mysql.jdbc.Driver")
      properties.put("user", "root")
      properties.put("password", "123456")
//    println(properties.getProperty("driver"))
//    println(properties.getProperty("user"))
//    println(properties.getProperty("password"))
      value.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://yb05:3306/songresult", "tm_mac_baseinfo_d", properties)














    }
}
