package day01

import org.apache.flink.streaming.api.scala._

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 15:35
 * @Name 多流转换 connect
 */
object Demo09 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val intStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)
    val stringStream: DataStream[String] = env.fromElements("a", "b", "c")
    val ci: ConnectedStreams[Int, String] = intStream.connect(stringStream)
    val si: DataStream[Any] = ci.map(i => i * i, _ + "a")
    si.print()
    env.execute()
  }
}
