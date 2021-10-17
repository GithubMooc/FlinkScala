package day01

import org.apache.flink.streaming.api.scala._
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 14:12
 * @Name 从集合读取数据
 */
object Demo03 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[List[SensorReading]] = env.fromElements(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    input.print("Stream>>>>>>>>>>")
    env.execute()
  }
}
