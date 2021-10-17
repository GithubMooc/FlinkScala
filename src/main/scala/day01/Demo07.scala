package day01

import func.MyFilter
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 15:01
 * @Name 转换算子 map 自定义filter keyBy minBy reduce
 */
object Demo07 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    //    读取数据
    val input: DataStream[String] = env.readTextFile("input/sensor.txt")
    //    map与filter，自定义Filter
    val dataStream: DataStream[SensorReading] = input
      .map(line => {
        val arr: Array[String] = line.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .filter(new MyFilter("sensor_1"))

    //    分组聚合
    //    简单滚动聚合，求每一个传感器所有温度值得最小值
    val aggStream: DataStream[SensorReading] = dataStream.keyBy(a => a.id).minBy("temperature")
    //    一般化聚合，输出SensorReading(id,最新时间戳+1,最小温度值)
    val reduceStream: DataStream[SensorReading] = dataStream.keyBy(a => a.id).reduce((a, b) => {
      SensorReading(a.id, a.timestamp + 1, a.temperature.min(b.temperature))
    })

    aggStream.print("agg")
    env.execute()
  }
}
