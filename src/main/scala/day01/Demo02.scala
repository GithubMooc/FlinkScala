package day01


import org.apache.flink.streaming.api.scala._

/**
 * @Author Master
 * @Date 2021/10/16
 * @Time 16:58
 * @Name 流处理 StreamWordCount
 */
object Demo02 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input:DataStream[String] = env.socketTextStream("localhost", 9999)
    val value = input.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(_._1).sum(1)
    value.print().setParallelism(1)
    env.execute()
  }
}
