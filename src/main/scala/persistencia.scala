import org.apache.log4j.{Level, LogManager}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext,storage}

/**
  * Created by carlos on 7/03/16.
  */
object persistencia {
  def main(args: Array[String]) {
    println("persist salva un RDD a disco o al nive que establezcams apra que en cada accion no tengamos que recalcular el RRDD lazy")
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","persiste",System.getenv("SPARK_HOME"))
    //create RRD form external data storage
    val data = sc.textFile("/home/carlos/Descargas/logs.txt")
    data.persist(StorageLevel.MEMORY_AND_DISK)//Podremos seleccionar le nivel en base a nuestras necesidades
    //with cache persistimos por defecto in ONLY_MEMORY
    //creamos un rrdd de errores
    val inputRDD = data.filter(f => f.contains("error")).cache()





    println(inputRDD.count())
    print(inputRDD.collect().mkString("\n"))

  }
}
