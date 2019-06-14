import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object SaprkApp extends App{
  val conf2 = new SparkConf().setAppName("spark asn.1").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf2)
  val conf = new Configuration(sc.hadoopConfiguration)
  val x =classOf[RawFileAsBinaryInputFormat]
  println("tessssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssst")
 // val rdd = sc.newAPIHadoopFile("data.txt", x,classOf[LongWritable], classOf[Text], conf)
//println(rdd)
}

