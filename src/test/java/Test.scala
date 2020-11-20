import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Test {
  var  sparkSession: SparkSession= _
  def main(args: Array[String]): Unit = {
    //    val a = 1
    ////    println(s"${a}")

    //    val list = List(1.3810752E+12, 1.3819392E+12, "2013-10-31 00:00:00", "")
    //      val a = 1.0143072E+12
    //            println(a.toLong)
    //        println(a.toDouble)
    //        val newTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(a.toLong))
    //        val newTime1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(a)
    //            println(newTime)
    //            println(newTime1)

    //        var str = "1.00006555E8"
    //        var a:Int = 0
    //        a = if(str.isEmpty) 0 else BigDecimal.valueOf(str.toDouble).toInt
    //        println(a)
    //        println(a.isValidInt)


    //    val decimal = BigDecimal.valueOf(a).toInt
    //    println(decimal)
    //     val a =  1.5 / 0


    //   var c5: String = "[{\"name\":\"《中国好声音第二季 第十五期》\",\"id\":\"5ce5ffdb2a0b8b6b4707263e\"}]"
    ////    val nObject = JSON.parseObject(c5)
    ////    c5 =  nObject.getString("name")
    //val array = JSON.parseArray(c5)
    //    val value = array.get(0)
    //    val nObject = JSON.parseObject(value.toString)
    //    val str = nObject.getString("name")
    //    println(str)

    //    val b = "[ada"
    //      if(b.contains("[") ||b.contains("]")){
    //        println("包含[ 或者]")
    //      }else{
    //        println("不包含")
    //      }


//        import java.util.regex.Pattern
//        val str = "1997-07-01 00:00:00"
//        val pattern = "^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d$"
//
//        val r = Pattern.compile(pattern)
//        val m = r.matcher(str)
//        System.out.println(m.matches)
//
//        println( new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(8.676864E+11))


    //    var c10 = "1.0143072E+12"
    //    var str = ""
    //    val pattern = "^[1-9]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d$"
    //    val r = Pattern.compile(pattern)
    //    val m = r.matcher(c10)
    //
    //      try {
    //        if (!m.matches) {
    //          c10 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c10.toDouble)
    //        }
    //      } catch {
    //        case exception: Exception =>  c10 = null
    //      }
    //
    //
    //
    //    println(c10)

    //    println(1.0143072E+12.toDouble.toLong)


    //    var c9 = "[{\"name\":\"胡海泉\",\"id\":\"10306\"},{\"name\":\"丁于\",\"id\":\"6311\"}]"
    //    var c9Name1 = ""
    //    var c9Name2 = ""
    //    var c9Id1 = ""
    //    var c9Id2 = ""
    //    try {
    //      //如果是[json] 格式用JSON.parseArray之后再用parseObject解析
    //      if (c9.contains("[") || c9.contains("]")) {
    //        val array = JSON.parseArray(c9)
    //        val value = array.get(0)
    //        val nObject = JSON.parseObject(value.toString)
    //        c9Name1 = nObject.getString("name")
    //        c9Id1 = nObject.getString("id")
    //        c9Name2 = nObject.getString("name")
    //        c9Id2 = nObject.getString("id")
    //        c9 = c9Name1 + "&" + c9Id1 + "\t"+c9Name2 +"&"+ c9Id2
    //      } else {
    //        //如果是json 格式用parseObject解析
    //        val nObject = JSON.parseObject(c9)
    //        c9Name1 = nObject.getString("name")
    //        c9Id1 = nObject.getString("id")
    //        c9 = c9Name1 + "&" + c9Id1 + "\t"+c9Name2 +"&"+ c9Id2
    //      }
    //    } catch {
    //      case exception: Exception => c9 = null
    //    }
    //
    //    println(c9)


    import java.util.regex.Pattern


    //    val pattern = "([\\[])(^([0-9]{1,}[.][0-9]*)$)"
    //
    //    val r = Pattern.compile(pattern)
    //    val m = r.matcher(str)
    //    System.out.println(m.matches)
    //    println(m.group(1))
//    val str = "[1.2,2.3]"
//    val str1 = str.substring(1, str.length - 1)
//    val strings: Array[String] = str1.split(",")

    //    val buffer = new mutable.ArrayBuffer[Any]()
    //          for(i<- strings){
    //              buffer.append(i.toDouble.toInt)
    //          }
    //
    //
    //      println(buffer)
    //    for(i<- buffer){
    //      println(i)
    //    }
//    val int = strings(0).toDouble.toInt
//    val int1 = strings(1).toDouble.toInt
//    println(int)
//    println(int1)

//
//    val aa ="[{\"name\":\"Stromae\",\"id\":\"4475\"},{\"name\":\"蓝心湄\",\"id\":\"15575\"}]"
//    val array = JSON.parseArray(aa)
//    val value = array.get(0)
//    val nObject: JSONObject = JSON.parseObject(value.toString)
//    val name: String = nObject.getString("name")
//    val id: String = nObject.getString("id")
//    val value1 = array.get(1)
//
//    val nObject1: JSONObject = JSON.parseObject(value1.toString)
//
//    val name1: String = nObject1.getString("name")
//    val id1: String = nObject1.getString("id")
//    var str = name + "&"+id+ "&"+name1+"&"+id1
//    println(str)



    //    var str = "abc"
    //   println("a".contains())



// sparkSession = SparkSession.builder().master("local[3]").appName("GenerateTwSongBaseinfoD")
//      .config("hadoop.home.dir", "/user/hive/warehouse")
//      .enableHiveSupport()
//      .getOrCreate()
//    import org.apache.sparkSession.implicits._
//    import sparkSession.implicits._
//  var times = "20201213"
//    val format = new SimpleDateFormat("yyyyMMdd")
//    val str: Date = format.parse(times)
//    val calendar: Calendar = Calendar.getInstance()
//    calendar.setTime(str)
//    calendar.add(Calendar.DAY_OF_MONTH, -7)
//    println(format.format(calendar.getTime))




    def getDataTime(times:String,num:Int): String ={
      val format = new SimpleDateFormat("yyyyMMdd")
      val date = format.parse(times)
      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      calendar.add(Calendar.DAY_OF_MONTH,-num)
      format.format(calendar.getTime)
    }

    println(getDataTime("20201210", 7))










  }
}

case class json(name: String, id: String)