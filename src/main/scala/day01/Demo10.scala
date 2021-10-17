package day01

import org.apache.flink.streaming.api.scala._


/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 16:06
 * @Name union连接多条流
 */
object Demo10 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1: DataStream[Int] = env.fromElements(1, 2)
    val stream2: DataStream[Int] = env.fromElements(3, 4)
    val stream3: DataStream[Int] = env.fromElements(5, 6)
    val union: DataStream[Int] = stream1.union(stream2, stream3)
    union.print("union")
    env.execute()
  }
}
