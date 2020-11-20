package com.spark_music.ok.utils

import java.text.SimpleDateFormat
import java.util.Calendar

object MyDataUtils {
  /**
   * 获取几天前的时间
   * @param times   传入的当前时间
   * @param num     几天前
   * @return
   */
  def getDataTime(times:String,num:Int): String ={
  val format = new SimpleDateFormat(times)
  val date = format.parse(times)
  val calendar = Calendar.getInstance()
  calendar.setTime(date)
  calendar.add(Calendar.DAY_OF_MONTH,-num)
  format.format(calendar.getTime)
}
}
