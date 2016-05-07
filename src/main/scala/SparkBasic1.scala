import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


/**
  * Created by carlos on 4/03/16.
  */
object SparkBasic1 {

    def main(args: Array[String]) {
        val conf = new SparkConf().setMaster("local").setAppName("Basic 1")
        val sc = new SparkContext(conf)
        val data = sc.textFile("/home/carlos/Descargas/logs.txt")
        val linesError = data.filter( l => l.contains("error"))

        println("Numeros de lineas con error : " + linesError.count())
    }
}
