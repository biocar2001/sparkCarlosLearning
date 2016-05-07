package rdd.key.values

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext

/**
  * Created by carlos on 8/03/16.
  */
object first {
  def main(args: Array[String]) {
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","operations with K,V",System.getenv("SPARK_HOME"))

    val data = sc.textFile("/home/carlos/Descargas/reduceByKeyExample.txt")
    val counts = data.map(line => (line.split(" ")(0),line))// devuelve un mapa con duplas de primera palabra como key y la linea entera como value

    println("Numero de palabras : " + counts.collect().mkString(""))

    val countsFiltred = counts.filter{case (x,y) => y.contains("mejor")}

    println("Numero de palabras filtradas: " + countsFiltred.collect().mkString(""))


  }

}
