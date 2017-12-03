import org.apache.spark._

//-Xms512m -Xmx1024m
//https://github.com/mpeltonen/sbt-idea
object FootDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Foot Driver")
    val sc = new SparkContext(conf)
    //    val champ = sc.textFile("champions.csv")

    //    val data = sc.textFile("epldata_final.csv")
    val champ = sc.textFile(args(0))
    val data = sc.textFile(args(1))
    val rdd1 = champ.map { line =>
      val v = line.split(",")
      (v(0), v(1).toInt)
    }
    rdd1.foreach(println)
    val rdd2 = data.filter(!_.contains("name")).map { line =>

      val v = line.split(",")
      (v(11), 1)

    }


    val res1 = rdd1.cogroup(rdd2).distinct()

    val res2 = res1.map {
      case (v1, (v2, v3)) => {
        if (!v2.isEmpty) {
          (v1 + " wins:", v2.toList(0) + " foot count: ", v3.toList.size)
        }
      }
    }
    val res3 = res2.map(s => s + ".").filter(!_.contains("().")).map {
      line => line.replaceAll("[(),]", " ")
    }.sortBy(line => line.split(" ").toList(1), true, 1)

    res3.saveAsTextFile(args(2))
    ////    // Split it up into words.
    //    val words = input.flatMap(line => line.split(","))
    //    // Transform into pairs and count.
    //    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    //    // Save the word count back out to a text file, causing evaluation.
    //    counts foreach println
    //    counts.saveAsTextFile("result")

  }
}
