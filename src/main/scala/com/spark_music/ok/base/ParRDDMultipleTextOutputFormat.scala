package com.spark_music.ok.base

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class ParRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = key.asInstanceOf[String]

//  key不要写入文件中
//  override def generateActualKey(key: Any, value: Any): Any = null
}
