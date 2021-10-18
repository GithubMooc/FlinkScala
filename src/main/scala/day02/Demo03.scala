package day02

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 23:50
 * @Name FlinkScala RedisSink
 */
object Demo03 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val input: DataStream[String] = env.readTextFile("input/sensor.txt")
        val data: DataStream[SensorReading] = input.map(line => {
            val arr: Array[String] = line.split(",")
            SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
        })
        data.addSink(
            new RedisSink[SensorReading](
                new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build(),
                new RedisMapper[SensorReading] {
                    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "sensor")

                    override def getKeyFromData(t: SensorReading): String = t.id

                    override def getValueFromData(t: SensorReading): String = t.temperature.toString
                }
            )
        )
        env.execute()
    }
}
