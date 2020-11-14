package com.spark_music

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class DefineFileName  extends MultipleTextOutputFormat{
  override def generateFileNameForKeyValue(key: Nothing, value: Nothing, name: String): String =
    key.asInstanceOf[String]
}
