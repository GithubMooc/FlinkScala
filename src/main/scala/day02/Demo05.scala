package day02

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/18
 * @Time 08:42
 * @Name FlinkScala 自定义Sink
 */
object Demo05 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val input: DataStream[String] = env.readTextFile("input/sensor.txt")
        val data: DataStream[SensorReading] = input.map(line => {
            val arr: Array[String] = line.split(",")
            SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
        })

        data.addSink(new RichSinkFunction[SensorReading] {
            var conn: Connection = _
            var insertStmt: PreparedStatement = _
            var updateStmt: PreparedStatement = _

            override def open(parameters: Configuration): Unit = {
                conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
                insertStmt = conn.prepareStatement("insert into sensor_temp (id, temperature) values (?, ?)")
                updateStmt=conn.prepareStatement("update sensor_temp set temperature = ? where id = ?")
            }

            override def invoke(value: SensorReading, context: SinkFunction.Context): Unit = {
//                直接执行更新语句，如果没有更新就插入
                updateStmt.setDouble(1, value.temperature)
                updateStmt.setString(2, value.id)
                updateStmt.execute()
                if( updateStmt.getUpdateCount == 0 ){
                    insertStmt.setString(1, value.id)
                    insertStmt.setDouble(2, value.temperature)
                    insertStmt.execute()
                }
            }

            override def close(): Unit = {
                insertStmt.close()
                updateStmt.close()
                conn.close()
            }
        })
        env.execute()
    }
}
