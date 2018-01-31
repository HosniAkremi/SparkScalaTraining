import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import com.typesafe.config._
object WordCount {
def main(args: Array[String]): Unit ={
  val props = ConfigFactory.load()
  val envProps = props.getConfig(args(0))
  val conf = new SparkConf().setAppName("Word Count")
    .setMaster(envProps.getString("executionMode"))
  val sc = new SparkContext(conf)
  val inputPath = args(1)
  val outputPath = args(2)
  val fs = FileSystem.get(sc.hadoopConfiguration)
  if(!fs.exists(new Path(inputPath))){
    println("Input Path does not exist")
  }else {
    if (fs.exists(new Path(outputPath)))
      fs.delete(new Path(outputPath), true)

    sc.textFile(inputPath).
      flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKey(_ + _).
      map(rec => rec._1 + "\t" + rec._2).
      saveAsTextFile(outputPath)

  }
}

}
