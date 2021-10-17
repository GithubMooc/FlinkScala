package func

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 16:15
 * @Name 自定义RichFlatmap
 */
class MyRichFlatMap extends RichFlatMapFunction[SensorReading, String] {
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def flatMap(in: SensorReading, collector: Collector[String]): Unit = {
    collector.collect(in.id)
    collector.collect("in")
  }

  override def close(): Unit = super.close()
}
