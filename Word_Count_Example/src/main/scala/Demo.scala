import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]) = {
    println("Hello, welcome to apache spark")

    //Create spark context with spark configuration
    val conf = new SparkConf().setAppName("Spark-Word-Count").setMaster("local")
    val sc = new SparkContext(conf)

    //read the text file
    //RDD[string]
    val textFile = sc.textFile("./src/main/resources/sampleText.txt")

    println("Text file read \n")

    //map each word in the textFile
    //RDD[Array[string]]
    val words = textFile.flatMap{ line => line.split(" ")}.map { word => (word, 1) }.reduceByKey(_ + _).saveAsTextFile("output.txt") //Save to a text file

    //println("Total count " + words.count())

    //stop the spark context
    sc.stop()

    //get threshold
    println("Welcome to Apache Spark")
  }
}