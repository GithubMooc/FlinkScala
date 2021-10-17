package day01

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import pojo.SensorReading

import scala.collection.immutable
import scala.util.Random

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 14:29
 * @Name flink
 */
object Demo06 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val input: DataStream[SensorReading] = env.addSource(new SourceFunction[SensorReading] {
      var running: Boolean = true

      override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        val random: Random = new Random()
        val curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i, 65 + random.nextGaussian() * 20))
        while (running) {
          curTemp.map(t => (t._1, t._2 + random.nextGaussian()))

          //        System.nanoTime()
          val curTime: Long = System.currentTimeMillis()

          curTemp.foreach(t => sourceContext.collect(SensorReading(t._1, curTime, t._2)))

          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    })
    input.print("Stream>>>>>>>>>>>")
    env.execute()
  }
}
