import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd
/**
  * Created by carlos on 7/03/16.
  */
object rdds {
  def main(args: Array[String]) {
    println("Creacion de RDDs")
    val sc = new SparkContext("local","rdds",System.getenv("SPARK_HOME"))
  //create RRD form external data storage
    val data = sc.textFile("/home/carlos/Descargas/logs.txt")

    // Transformacion creacion de nuevo RDD en modo lazy - la transformacion(map,fileter,etc) siempre devuelve un nuevo RDD
    // que en realidad solo seran computados cuando se invoque alguna accion sobre ellos(filter,count,etc)
    val errorValues = data.filter(f => f.contains("error"))
      .cache()// es lo mismo que persist guardando en disco el RDD para su posterior uso

    //Accion obetniendo un resultado
    println(errorValues.count())

    //Con parallelize metemos en memoria un nuevo RDD creado desde datos locales - con collect obtenemos una nueva coleccion:http://www.scala-lang.org/files/archive/nightly/docs/library/index.html#scala.collection.TraversableLike
    val lines = sc.parallelize(errorValues.collect())//No usar collect en grndes datasets ya que estaos cargando todo en memoria

    print(lines.first())

  }
}
