

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext

/**
  * Created by carlos on 7/03/16.
  */
object rddsUnion {
  def main(args: Array[String]) {
    println("Creacion de RDDs usando union")
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","rdds",System.getenv("SPARK_HOME"))
    //create RRD form external data storage
    val data = sc.textFile("/home/carlos/Descargas/logs.txt")

    //creamos un rrdd de errores
    val inputRDD = data.filter(f => f.contains("error")).cache()
    println(inputRDD.count())
    //creamos un rrdd de notice
    val inputRDDN = data.filter(f => f.contains("notice")).cache()
    println(inputRDDN.count())
    //usamos union de 2 RDDS
    val RDDUnion = inputRDD.union(inputRDDN)

    print(RDDUnion.count())

  }
}
