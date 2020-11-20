package com.spark_music.my

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class DefineFileName  extends MultipleTextOutputFormat[Any,Any]{
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}
