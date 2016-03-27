package solution

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.v09.KafkaUtils

object HBaseSensorStream extends Serializable {
  final val tableName = "/user/user01/sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfAlertBytes = Bytes.toBytes("alert")
  final val colHzBytes = Bytes.toBytes("hz")
  final val colDispBytes = Bytes.toBytes("disp")
  final val colFloBytes = Bytes.toBytes("flo")
  final val colSedBytes = Bytes.toBytes("sedPPM")
  final val colPsiBytes = Bytes.toBytes("psi")
  final val colChlBytes = Bytes.toBytes("chlPPM")

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

  object Sensor extends Serializable {
    // function to parse line of sensor data into Sensor class
    def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    }
    //  Convert a row of sensor object data to an HBase put object
    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object 
      put.addColumn(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
      put.addColumn(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
      put.addColumn(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
      put.addColumn(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
      put.addColumn(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      put.addColumn(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }
    // convert psi alert to an HBase put object
    def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      // create a composite row key: sensorid_date time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))
      // add to column family alert, column psi data value to put object 
      p.addColumn(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }
  }

  def main(args: Array[String]): Unit = {

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "testgroup"
    val offsetReset = "earliest"

    val pollTimeout = "1000"
    val topics = "/user/user01/pump:sensor"

    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    println("set configuration")
    val sparkConf = new SparkConf().setAppName("HBaseSensorStream")
      .set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)

    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sc, Seconds(2))

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

    // parse the lines of data into sensor objects  
    val messages = KafkaUtils.createDirectStream[String, String](ssc, kafkaParams, topicsSet)

    val sensorDStream = messages.map(_._2).map(Sensor.parseSensor)

    sensorDStream.print()

    sensorDStream.foreachRDD { rdd =>
      // filter sensor data for low psi
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
      alertRDD.take(1).foreach(println)
      // convert sensor data to put object and write to HBase table column family data
      rdd.map(Sensor.convertToPut).
        saveAsHadoopDataset(jobConfig)
      // convert alert data to put object and write to HBase table column family alert
      alertRDD.map(Sensor.convertToPutAlert).
        saveAsHadoopDataset(jobConfig)
    }
    // Start the computation
    ssc.start()
    println("start streaming")
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}