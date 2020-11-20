import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils



object Test02 {
  def main(args: Array[String]): Unit = {
//    sparkSession = SparkSession.builder().appName("ProduceClientLog").master("local[4]").enableHiveSupport().getOrCreate()
//    sc = sparkSession.sparkContext
//    val array = Array(Array(List(1, 2, 3), List(30, 20)))
//    val value: RDD[Array[List[Int]]] = sc.parallelize(array)
//



  }


  /**
   * 根据经纬度获取详细地址
   */
  def getAddressFromXY(x:String,y:String): Unit ={

    val httpClient: CloseableHttpClient = HttpClients.createDefault()
    val httpGet  = new HttpGet(s"https://restapi.amap.com/v3/geocode/regeo?&location=${x},${y}&key=f6d2adebb4765ae57e0ebdc5210d6a3e")
    // 发送请求，获取返回信息
    val response: CloseableHttpResponse =  httpClient.execute(httpGet)


    val entity: HttpEntity = response.getEntity
  if(response.getStatusLine.getStatusCode == 200){

    // 将返回对象中数据转换为字符串
    val resultStr: String = EntityUtils.toString(entity)

    // 解析返回的json字符串
    val jSONObject: JSONObject = JSON.parseObject(resultStr)

  }





  }

}
