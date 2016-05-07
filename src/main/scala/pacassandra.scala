
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
object pacassandra {
  def main(args: Array[String]) {
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(List(Seq("moremagic", 1)))
    rdd.saveToCassandra("sparkCarlos" , "kv", SomeColumns("key", "value"))
    // Read entire table as an RDD. Assumes your table test was created as
    // CREATE TABLE test.kv(key text PRIMARY KEY, value int);
    val data = sc.cassandraTable("sparkCarlos" , "kv")
    // Print some basic stats on the value field.
    data.map(row => row.getInt("value")).stats()
    println("Veamos: " + data.collect)
  }
  
}