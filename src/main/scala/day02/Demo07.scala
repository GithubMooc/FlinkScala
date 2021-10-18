package day02

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/18
 * @Time 11:28
 * @Name FlinkScala 不同类型的窗口
 */
object Demo07 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //        val input: DataStream[String] = env.readTextFile("input/sensor.txt")
        val input: DataStream[String] = env.socketTextStream("localhost", 9999)
        val data: DataStream[SensorReading] = input.map(line => {
            val arr: Array[String] = line.split(",")
            SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
        })
        val result: DataStream[(String, Double, Long)] = data
            .map(data => (data.id, data.temperature, data.timestamp))
            .keyBy(_._1)
            //            .window(TumblingProcessingTimeWindows.of(Time.seconds(15))) //滚动时间窗口
            //            .window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(3))) //滑动时间窗口
            //            .window(ProcessingSessionWindows.withGap(Time.seconds(10))) //会话窗口 不能简化
            //            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))) //会话窗口 固定gap
            //            .window(ProcessingTimeSessionWindows.withDynamicGap((t: (String, Double, Long)) => t._3 * 1000L)) //会话窗口，动态gap
            //            .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[Tuple3[String,Double,Long]] {
            //                            override def extract(t: (String, Double, Long)): Long = {
            //                                return t._3*1000L
            //                            }
            //                        })) //会话窗口，动态gap
            //            .timeWindow(Time.seconds(15))
            //            .timeWindow(Time.seconds(15),Time.seconds(3))
            //            .countWindow(10) //滚动计数窗口
            //            .countWindow(10,2) //滑动计数窗口
            .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
            .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
        result.print()
        env.execute()
    }
}
