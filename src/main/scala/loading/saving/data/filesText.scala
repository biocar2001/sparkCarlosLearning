package loading.saving.data

import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.SparkContext._

/**
  * Created by carlos on 10/03/16.
  */
object filesText {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","Text Files",System.getenv("SPARK_HOME"))

    //val data = sc.textFile("/home/carlos/Descargas/reduceByKeyExample.txt")
    //SparkContext.wholeTextFiles() method and get back a pair RDD where the key is the name of the input file.
    val data = sc.wholeTextFiles("/home/carlos/Descargas/09032016.txt")
    val result = data.mapValues{y =>
      val nums = y.split(" ").map(x => x.toString())
      nums
    }
    result.foreach(println)
  }
}
