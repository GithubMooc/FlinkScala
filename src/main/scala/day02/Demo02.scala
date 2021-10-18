package day02

import java.util.Properties

import org.apache.flink.api.common.serialization._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 23:30
 * @Name FlinkScala 读取Kafka sensor，处理后写入Kafka sinktest
 */
object Demo02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val input: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))
    val data: DataStream[SensorReading] = input.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })
    //    data.map(_.toString).addSink(new FlinkKafkaProducer[String]("localhost:9092","sinktest",new SimpleStringSchema()))
    data.map(data => (data.id, data.temperature)).addSink(new FlinkKafkaProducer[(String, Double)]("localhost:9092", "sinktest", (t: (String, Double)) => s"id:${t._1} temp:${t._2}".toArray.map(_.toByte)))
    env.execute()
  }
}
