package rdds.operations.basic

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext

/**
  * Created by carlos on 7/03/16.
  */
object rddBasic {

  def main(args: Array[String]) {
    println("RDD Basic")
    LogManager.getRootLogger.setLevel(Level.ERROR)//set nivel de error en la consola
    val sc = new SparkContext("local","rdds basic",System.getenv("SPARK_HOME"))

    //operaciones basicas
    // Map y filter
    val data=  sc.parallelize(List(1,2,3,4))
    val dataMap = data.map(x => x*x)
    println("********** Map RESULT: **************************************")
    println(dataMap.collect().mkString(","))
    println("************************************************")

    val datafilter = data.filter(f => f % 2 == 0)

    println("*************Filter Result: ***********************************")
    println(datafilter.collect().mkString(","))
    println("************************************************")


    val dataText = sc.textFile("/home/carlos/Descargas/logs.txt")
    // La diferencia entre map y flatmap es que un flatmap devolveria un RDD de cada uno de los elementos de otro RDD
    /* dado una lista/RDD {"carlos mola","es el mas","mejor de todos los tiempos"}si aplicamos un map seria e] siguiente RDD
    {["carlos mola"],["es el mas"],["mejor de todos los tiempos"]} en cambioaplicandole un flatmap quedaria un

     */
    val flatData = dataText.flatMap(x => x.split(" "))
    println("**************split result **********************************")
    println(flatData.collect().mkString("\n"))


    println("**************cartesians perations**********************************")
    println("disticnt, elimna repetids pero requiere shuffling suponiendo un alto coste de renddiemiento: ")
    val distinctData = flatData.distinct()
    println(distinctData.collect().mkString("\n"))
    println("Union, no elimina los repetidos, pero junta 2 RDDs")
    println(flatData.union(distinctData).collect().mkString("\n"))
    println("intersection, da una union de ambos pero con alto gasto de memoria: ")
    println(flatData.intersection(distinctData).collect().mkString("\n"))
    println("subtract, elimna aquellos que estan en el segundo y ya en el primero: ")
    println(flatData.subtract(distinctData).collect().mkString("\n"))

    println("***************                ACCIONES              *********************************")


    println("********************* Reduce: ***************************")
    val datareduce = List(1,2,3,4,5,6)

    val mapred =  datareduce.map(x => x+x).reduce((x,y) => x+y)
    println(mapred)
    println("*********************count,countByKey,take,top,sum,reduceByKey son tambien acciones junto con aggregate que le determinads eltipo de salida1***************************")
    println("********************* Agreggate: ***************************")
    val datareduceAgrre = sc.parallelize(List("12","23","","456"),2)

    val mapredA =  datareduceAgrre.aggregate("")((x,y) => math.min(x.length,y.length).toString(),(x,y) => x+y)

    println(mapredA)


  }
}
