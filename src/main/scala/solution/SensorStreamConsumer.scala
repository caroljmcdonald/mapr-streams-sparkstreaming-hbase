package solution

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{ DStream, InputDStream }
import org.apache.spark.streaming.kafka.v09.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SQLContext

object SensorStreamConsumer extends Serializable {

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }
  val timeout = 10 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "testgroup"
    val offsetReset = "earliest"
    val batchInterval = "2"
    val pollTimeout = "1000"
    val topics = "/user/user01/pump:sensor"

    val sparkConf = new SparkConf().setAppName("SensorStream")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val sensorDStream = messages.map(_._2).map(parseSensor)

    sensorDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        import org.apache.spark.sql.functions._

        val sensorDF = rdd.toDF()
        // Display the top 20 rows of DataFrame
        println("sensor data")
        sensorDF.show()
        sensorDF.registerTempTable("sensor")
        val res = sqlContext.sql("SELECT resid, date, count(resid) as total FROM sensor GROUP BY resid, date")
        println("sensor count ")
        res.show
        val res2 = sqlContext.sql("SELECT resid, date, avg(psi) as avgpsi FROM sensor GROUP BY resid,date")
        println("sensor psi average")
        res2.show

      }
    }
    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}