package day01

import org.apache.flink.api.scala._


/**
 * @Author Master
 * @Date 2021/10/16
 * @Time 16:43
 * @Name 批处理 WordCount
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    //    创建执行环境
    //    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    从文件中读取数据
    //    val inputDS:DataSet[String] = env.readTextFile("input/hello.txt")
    val inputDS = env.readTextFile("input/hello.txt")
    val result = inputDS.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    result.print()
  }
}
