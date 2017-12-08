

object TwitterDriver {

  def main(args: Array[String]) {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import org.apache.spark.streaming.twitter._


    if (args.length < 4) {
      System.err.println("Usage: PopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("PopularTags").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("C:\\Users\\user\\IdeaProjects\\ScalaSpark\\check")
    val stream = TwitterUtils.createStream(ssc, None, filters)
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (topic, count) }
   topCounts10.updateStateByKey(updateFunction _)
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(count, tag)) }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
