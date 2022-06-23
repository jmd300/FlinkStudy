package scala.chapter02

import org.apache.flink.streaming.api.scala._

/**
 * @author guguowei 2022/6/23 周四
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataStream = env.readTextFile("/Users/guguowei/github/FlinkStudy/src/main/resources/WordCount.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    dataStream.print
    env.execute()
  }
}
