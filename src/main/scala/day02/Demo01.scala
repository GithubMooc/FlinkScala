package day02

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 22:24
 * @Name Sink 写入Kafka
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val input: DataStream[String] = env.socketTextStream("localhost", 9999)
    input.addSink(new FlinkKafkaProducer[String]("localhost:9092","test",new SimpleStringSchema()))
    env.execute()
  }
}
