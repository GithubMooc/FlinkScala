package day02

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/18
 * @Time 00:11
 * @Name FlinkScala ElasticSearch7 Sink
 */
object Demo04 {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val input: DataStream[String] = env.readTextFile("input/sensor.txt")
        val data: DataStream[SensorReading] = input.map(line => {
            val arr: Array[String] = line.split(",")
            SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
        })
        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("localhost",9999))
        data.addSink(new ElasticsearchSink
        .Builder[SensorReading](httpHosts,new ElasticsearchSinkFunction[SensorReading] {
            override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                val dataSource = new util.HashMap[String, String]()
                dataSource.put("id", t.id)
                dataSource.put("temp", t.temperature.toString)
                dataSource.put("ts", t.timestamp.toString)

                // 创建index request
                val indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .id("temperature")
//                    .`type`("temperature")
                    .source(dataSource)

                // 利用RequestIndexer发送http请求
                requestIndexer.add(indexRequest)

                println(t + " saved")
            }
        }).build())
    }
}
