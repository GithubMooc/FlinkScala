package func

import org.apache.flink.api.common.functions.MapFunction
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 16:13
 * @Name 自定义MapFunction
 */
class MyMap extends MapFunction[SensorReading, (String, Double)] {
  override def map(value: SensorReading): (String, Double) = (value.id, value.temperature)
}
