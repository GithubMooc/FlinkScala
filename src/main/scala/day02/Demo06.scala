package day02

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/18
 * @Time 10:28
 * @Name FileSink
 */
object Demo06{
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val input: DataStream[String] = env.readTextFile("input/sensor.txt")
        val data: DataStream[SensorReading] = input.map(line => {
            val arr: Array[String] = line.split(",")
            SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
        })

        data.addSink(StreamingFileSink.forRowFormat(new Path("output"),new SimpleStringEncoder[SensorReading]()).build())
        env.execute()
    }
}
