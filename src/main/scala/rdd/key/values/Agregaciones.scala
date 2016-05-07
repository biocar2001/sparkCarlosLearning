package rdd.key.values

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext

/**
  * Created by carlos on 8/03/16.
  */
object Agregaciones {

  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","Agregciones K,V",System.getenv("SPARK_HOME"))

    /*val map = Map(1 -> "apricot",2 -> "banana",3 -> "clementine",4 -> "durian",5 -> "fig",6 -> "guava",7 -> "jackfruit",8 -> "kiwi",9 -> "lime", 10 -> "mango")
    val mapV = map.mapValues(x => x+"Carlos")
    val red = mapV.*/

    val map = Map(1 -> "apricot",1 -> "apricot",2 -> "banana",3 -> "clementine",4 -> "durian",5 -> "fig",6 -> "guava",7 -> "jackfruit",8 -> "kiwi",9 -> "lime", 10 -> "mango")
    val mapV = map.mapValues(x => x+"Carlos")
    val rdd = sc.parallelize((1 to 10000).map(x=>(x.toLong,0)))

    /*val filtred = rdd.reduceByKey((accum,x)=>(accum+x))
    print(filtred.collect)*/

  }

}
