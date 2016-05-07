package rdd.key.values

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext

/**
  * Created by carlos on 9/03/16.
  */
object joins {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","Joins",System.getenv("SPARK_HOME"))



    /*val storeAddress = List(
      (Store("Ritual"), "1026 Valencia St"), (Store("Philz"), "748 Van Ness Ave"),
      (Store("Philz"), "3101 24th St"), (Store("Starbucks"), "Seattle"))
    val storeRating = List(
      (Store("Ritual"), 4.9), (Store("Philz"), 4.8))

    storeAddress.join(storeRating) == {
      (Store("Ritual"), ("1026 Valencia St", 4.9)),
      (Store("Philz"), ("748 Van Ness Ave", 4.8))


    print(filtred.collect)*/

  }
}

case class Store(s: String)
