import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.Row
import org.json.JSONObject

import scala.util.control.Breaks._
object Test03 {
  def main(args: Array[String]): Unit = {
//    breakable{
//
//      for(i <- 1 to 3){
//        if(i == 2){
//          break()
//        }
//        println(i)
//      }
//    }

//
//
//    for(i <- 1 to 3 if i == 2){
//      println(i)
//    }

    def getDataTime(times:String,num:Int): String ={
      val format = new SimpleDateFormat(times)
      val date = format.parse(times)
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.add(Calendar.DAY_OF_MONTH,-num)
      format.format(calendar.getTime)
    }




  }
}
