package com.spark_music.ok.utils

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils {
  //load默认读取是 resources中的application.conf、
  // application。properties或application.json文件
  //  识别.json    .conf  文件 默认读取
  private val load: Config = ConfigFactory.load()
  val HIVE_DATABASE_NAME: String = load.getString("hive.database.name")
  val IF_LOCAL: Boolean = load.getBoolean("iflocal")
  val HDFS_PATH: String = load.getString("hdfs.path")
  val MYSQL_USERNAME: String = load.getString("mysql.username")
  val MYSQL_PASSWORD: String = load.getString("mysql.password")
  val MYSQL_JDBC_URL: String = load.getString("mysql.jdbc.url")
}
