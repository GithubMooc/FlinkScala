package day01

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 14:22
 * @Name 从文件中读取数据
 */
object Demo04 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val input: DataStream[String] = env.readTextFile("input/sensor.txt")
    input.print(">>>>>>>>>>>")
    env.execute()
  }
}
