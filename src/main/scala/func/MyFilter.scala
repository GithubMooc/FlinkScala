package func

import org.apache.flink.api.common.functions.FilterFunction
import pojo.SensorReading

/**
 * @Author Master
 * @Date 2021/10/17
 * @Time 15:16
 * @Name 自定义FilterFunction
 */
class MyFilter(k: String) extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = t.id.startsWith(k)
}