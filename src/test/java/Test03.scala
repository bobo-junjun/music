import org.apache.spark.sql.Row
import org.json.JSONObject

import  scala.util.control.Breaks._
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



    for(i <- 1 to 3 if i == 2){
      println(i)
    }

  }
}
