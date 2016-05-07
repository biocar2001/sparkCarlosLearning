import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd
/**
  * Created by carlos on 4/03/16.
  */
object WordCount{
  def main(args: Array[String]) {

    val sc = new SparkContext("local","WordCount",System.getenv("SPARK_HOME"))

    val data = sc.textFile("/home/carlos/Descargas/logs.txt")
    val counts = data.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey{case (x,y) => x+y}

    println("Numero de palabras : " + counts.count())


  }
}
