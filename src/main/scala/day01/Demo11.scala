package day01

import org.apache.flink.streaming.api.scala._


/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 22:38
 * @Name sum使用
 */
object Demo11 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val input: DataStream[(Int, Int)] = env.fromElements((1, 2), (1, 3))
    input.keyBy(_._1).sum(1).print("sum")
    env.execute()
  }

}
