package org.sandbox.wiki

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status

import scala.collection.mutable.HashMap
import scala.io.{StdIn, Source}

import org.apache.spark.streaming._

import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

import scala.util.Random
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
  * Created by ariellev on 26.12.15.
  */
object Wiki {
  def configureTwitterCredential() = {
    val file = new File("twitter.txt")
    if (!file.exists) {
      throw new Exception("could not find twitter. cannot obtain access token")
    }
    val lines = Source.fromFile(file).getLines().filter(_.trim.size > 0).toSeq
    val pairs = lines.map(line => {
      val split = line.split("=")
      if (split.size != 2)
        throw new Exception("Error parsting twitter.txt. incorrect formatted line [" + line + "]")
      (split(0).trim, split(1).trim)
    })

    val map = new HashMap[String, String] ++= pairs
    val configKeys = Seq("consumerKey", "consumerSecret", "accessToken", "accessTokenSecret")
    configKeys.foreach(key => {
      if (!map.contains(key)) {
        throw new Exception("Error setting OAuth authentication key [" + key + "] not found")
      }
      val fullKey = "twitter4j.oauth." + key
      System.setProperty(fullKey, map(key))
      println("\tProterty " + fullKey + "set as " + map(key))
    })
    println()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions = model.predict(data.map(r => (r.user, r.product))).map(r => r.rating)
    val validations = data.map(r => r.rating)
    Math.sqrt(validations.subtract(predictions).map(Math.pow(_, 2)).mean)
  }

  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    movies.map({
      case (id, name) => {
        println("Please rate the following movie (1-5 (best), or 0 if not seen):")
        print(name + ":")
        Rating(0, id, StdIn.readInt())
      }
    })
  }

  def main(args: Array[String]) = {
    if (args.length < 4) {
      println("usage: [master] [switch] [input] [output]")
      System.exit(1)
    }

    //    val input = "/Users/ariellev/myWS/sandbox/spark/amp-camp/data/pagecounts/part-00100"
    val master = args(0)
    val switch = args(1)
    val input = args(2)
    val output = args(3)
    val sparkHome = sys.env("SPARK_HOME")
    val jarFile = "target/scala-2.11/wiki_2.11-1.0.jar"
    val app = s"ampcamp-$switch"


    println(s"staring [$switch], input=$input, output=$output")
    if (switch == "site_counts") {
      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))
      val pagecounts = sc.textFile(input)
      val enPages = pagecounts.filter(line => line.split(" ")(1) == "en").cache()
      //val dateCounts = enPages.map( line => { val split = line.split(" "); (split(0).split("-")(0), split(3).toInt) })
      val siteCounts = enPages.map(line => {
        val split = line.split(" ");
        (split(2), split(3).toInt)
      }).partitionBy(new HashPartitioner(4))
      // val histDate = dateCounts.reduceByKey( (x,y) => x + y)
      val histSite = siteCounts.reduceByKey((x, y) => x + y, 4)
      histSite.saveAsTextFile(output)
      sc.stop

    } else if (switch == "sql") {
      // sbt "run local[4] sql /Users/ariellev/myWS/sandbox/spark/amp-camp/data/pagecounts/ output-sql"

      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))
      val hiveCtx = new HiveContext(sc)
      import hiveCtx.implicits._
      val frame = hiveCtx.sql(s"create external table wikistats (dt string, project_code string, page_name string, page_views int, bytes int) row format delimited fields terminated by ' ' location '$input'")
      val select = hiveCtx.sql(s"select page_name, sum(page_views) as views from wikistats  where project_code = 'en' group by page_name order by views desc")
      select.rdd.saveAsTextFile(output)
      sc.stop

    } else if (switch == "twitter") {
      // sbt "run local[4] twitter in output-twitter"
      configureTwitterCredential()
      val checkpointDir = "checkpoint"
      val ssc = new StreamingContext(master, app, Seconds(1), sparkHome, Seq(jarFile))
      val tweets = org.apache.spark.streaming.twitter.TwitterUtils.createStream(ssc, None)
      val statuses = tweets.map(status => status.getText())
      val words = statuses.flatMap {
        _.split(" ")
      }
      val hashTags = words.filter(_.startsWith("#"))

      // sliding a 2-min window every 30 seconds
      val counts = hashTags.map(tag => (tag, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(2 * 60), Seconds(30))
      val sorted = counts.map { case (tag, count) => (count, tag) }.transform(rdd => rdd.sortByKey())
      sorted.foreachRDD(rdd => println("\nTop 10 hash tags:\n" + rdd.take(10).mkString("\n")))
      sorted.saveAsTextFiles(output + "/stream")
      ssc.checkpoint(checkpointDir)
      ssc.start()
      ssc.awaitTermination()

    } else if (switch == "mlib") {

      // sbt "run local[4] mlib ../data/movielens/medium output-mlib"
      // http://ampcamp.berkeley.edu/big-data-mini-course/movie-recommendation-with-mllib.html

      // loading data
      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))
      val ratings = sc.textFile(input + "/ratings.dat").map { line =>
        val fields = line.split("::")
        (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
      }

      val movies = sc.textFile(input + "/movies.dat").map { line =>
        val fields = line.split("::")
        (fields(0).toInt, fields(1))
      }.collect.toMap

      val numRatings = ratings.count
      val numUsers = ratings.map(_._2.user).distinct.count
      val numMovies = ratings.map(_._2.product).distinct.count
      println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

      // finding 50 most rated movies

      // my solution:
      // val top50 = ratings.values.map(r => r.product -> r.rating).reduceByKey(_+_).map{case(product, raiting) => (raiting, product)}.sortByKey(false).take(50)
      // println(top50.foreach( p => println("\t\t\t\t"+movies(p._2) + "\t" + p._1)))

      // http://ampcamp.berkeley.edu/big-data-mini-course/movie-recommendation-with-mllib.html

      val mostRatedMovieIds = ratings.map(_._2.product) // extract movie ids
        .countByValue // count ratings per movie
        .toSeq // convert map to Seq
        .sortBy(-_._2) // sort by rating count
        .take(50) // take 50 most rated
        .map(_._1) // get their ids

      // sampling 5% to elicitate user's selection
      val random = new Random(0)
      val selectedMovides = mostRatedMovieIds.filter(x => random.nextDouble() < 0.2).map(x => (x, movies(x))).toSeq
      val myRating = elicitateRatings(selectedMovides)
      val myRaitingRDD = sc.parallelize(myRating)

      // splitting data exclusively into training, validation and test set (60%, 20%, 20%)
      // adding myRaiting to training set,
      // repartitioning
      val numPartitions = 20
      val training = ratings.filter(x => x._1 < 6).values.union(myRaitingRDD).repartition(numPartitions).persist
      val validation = ratings.filter(x => x._1 >= 8).values.repartition(numPartitions).persist
      val test = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).persist

      val numTraining = training.count
      val numValidation = validation.count
      val numTest = test.count

      println(s"Training: $numTraining , Validation: $numValidation, Test: $numTest")

      val ranks = List(8, 12)
      val lambdas = List(0.1, 10.0)
      val iterations = List(10, 20)

      var bestValidationRmse = Double.MaxValue
      var bestModel: Option[MatrixFactorizationModel] = None
      var bestRank = 0
      var bestIteration = -1
      var bestLambda = -1.0

      for (r <- ranks; it <- iterations; l <- lambdas) {
        val model = ALS.train(training, r, it, l)
        val rmse = computeRmse(model, validation, numValidation)
        println("RMSE (validation) = $rmse for the model trained with rank = $r, lambda = $l, and numIter = $it.")
        if (rmse < bestValidationRmse) {
          bestValidationRmse = rmse
          bestModel = Some(model)
          bestRank = r
          bestIteration = it
          bestLambda = l
        }
      }

      val testRmse = computeRmse(bestModel.get, test, numTest)
      println(s"The best model was trained using rank $bestRank and lambda $bestLambda, and its RMSE on test is $testRmse")

      var i = 0
      bestModel.get.recommendProducts(0, 50).foreach({
        case (Rating(user, product, rating)) => println(s"$i: " + movies(product))
          i = i + 1
      })

      sc.stop


    } else if (switch == "graph") {
      // http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html

      val vertexArray = Array(
        (1L, ("Alice", 28)),
        (2L, ("Bob", 27)),
        (3L, ("Charlie", 65)),
        (4L, ("David", 42)),
        (5L, ("Ed", 55)),
        (6L, ("Fran", 50))
      )
      val edgeArray = Array(
        Edge(2L, 1L, 7),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(5L, 6L, 3)
      )
      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))

      val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
      val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
      val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

      graph.vertices.values.filter(_._2 > 30).foreach { case (name, age) => println(s"$name is $age years old") }
      graph.triplets.filter(_.attr > 0).foreach(edge => if (edge.attr <= 5) println(s"${edge.srcAttr._1} likes ${edge.dstAttr._1}") else println(s"${edge.srcAttr._1} likes ${edge.dstAttr._1} !"))

      // transforming vertices into user classes and incorporating external information e.g in/out degree
      case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
      val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }

      val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) { case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
      }.outerJoinVertices(initialUserGraph.outDegrees) { case (id, u, degOpt) => User(u.name, u.age, u.inDeg, degOpt.getOrElse(0)) }

      userGraph.vertices.foreach { case (id, u) => println(s"User $id is called ${u.name} and is liked by ${u.inDeg} people") }

      // Print the names of the users who are liked by the same number of people they like.
      for ((id, user) <- userGraph.vertices.collect; if user.inDeg == user.outDeg) {
        println(s"User $id, ${user.name}, likes and is liked by the same amount of people (${user.inDeg})")
      }

      // Find the oldest follower for each user
      val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))), (x, y) => if (x._2 > y._2) x else y)
      val joined = oldestFollower.innerJoin[User, (String, String, Int)](userGraph.vertices) {
        case (id, (follower, age), u) => (u.name, follower, age)
      }

      joined.foreach { case (id, (user, follower, age)) => println(s"$follower is the oldest follower of ${user} ($age)") }

      // course solution
      //      userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      //        optOldestFollower match {
      //          case None => s"${user.name} does not have any followers."
      //          case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      //        }
      //      }.collect.foreach { case (id, str) => println(str) }

      // As an exercise, try finding the average follower age of the followers of each user.
      val averageFollower: VertexRDD[Double] = userGraph.mapReduceTriplets[(Double, Int)](edge => Iterator((edge.dstId, (edge.srcAttr.age.toDouble, 1))), (x, y) => (x._1 + y._1, x._2 + y._2)).mapValues((id, p) => p._1 / p._2)
      userGraph.vertices.leftJoin(averageFollower) { (id, user, optAverageFollower) =>
        optAverageFollower match {
          case None => s"${user.name} doesn't have followers"
          case Some(avg) => s"${user.name}'s followers' average age is ${avg}"
        }
      }.values.foreach(println)

      // sub graph
      val olderGraph = userGraph.subgraph(vpred = (id, user) => user.age >= 30)

      // compute the connected components
      val cc = olderGraph.connectedComponents

      // display the component id of each user:
      olderGraph.vertices.leftJoin(cc.vertices) {
        case (id, user, comp) => s"${user.name} is in component ${comp.get}"
      }.collect.foreach { case (id, str) => println(str) }
      sc.stop
    }
    else if (switch == "wiki_links") {
      // sbt "run local[4] wiki_links ../data/wiki_links output-wiki_links"

      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))

      // We tell Spark to cache the result in memory so we won't have to repeat the
      // expensive disk IO. We coalesce down to 20 partitions to avoid excessive
      // communication.
      val wiki: RDD[String] = sc.textFile(input + "/part*1").coalesce(20)
      println(wiki.first)

      case class Article(val title: String, val body: String)
//      var articles = wiki.map(line => {
//        val split = line.split("\t")
//        Article(split(0).trim, split(1).trim)
//      })

      // course solution
      // Parse the articles
      val articles = wiki.map(_.split('\t')).
        // two filters on article format
        filter(line => (line.length > 1 && !(line(1) contains "REDIRECT"))).
        // store the results in an object for easier access
        map(line => new Article(line(0).trim, line(1).trim)).cache

      // Hash function to assign an Id to each article
      def pageHash(title: String): VertexId = {
        title.toLowerCase.replace(" ", "").hashCode.toLong
      }

      // The vertices with id and article title:
      val vertices: RDD[(VertexId, String)] = articles.map(article => (pageHash(article.title), article.title)).cache

      // The next step in data-cleaning is to extract our edges to find the structure of the link graph.
      // We know that the MediaWiki syntax (the markup syntax Wikipedia uses) indicates a link by
      // enclosing the link destination in double square brackets on either side.
      // So a link looks like “[[Article We Are Linking To]].” Based on this knowledge,
      // we can write a regular expression to extract all strings that are enclosed on both
      // sides by “[[” and “]]” respectively, and then apply that regular expression to each article’s contents,
      // yielding the destination of all links contained in the article.

      val pattern = "\\[\\[.+?\\]\\]".r
      val edges: RDD[Edge[Double]] = articles.flatMap(a => {
        val srcId = pageHash(a.title)
        pattern.findAllIn(a.body).map {
          link =>
            val distId = pageHash(link.replace("[[", "").replace("]]", ""))
            Edge(srcId, distId, 1.0)
        }
      })

      val graph = Graph(vertices, edges, "").subgraph(vpred = (hash, title) => title.nonEmpty).cache
      val prGraph = graph.staticPageRank(5).cache

      val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
        case (id, title, rank) => (rank.getOrElse(0.0), title)
      }

      val top10 = titleAndPrGraph.vertices.values.sortBy(-_._1).saveAsTextFile(output + "-top10")

        //.foreach(t => println(t._2._2 + ": " + t._2._1))
      val berkeleyGraph = graph.subgraph(vpred = (v, t) => t.toLowerCase contains "berkeley")

      val prBerkeley = berkeleyGraph.staticPageRank(5).cache

      berkeleyGraph.outerJoinVertices(prBerkeley.vertices) {
        (v, title, r) => (r.getOrElse(0.0), title)
      }.vertices.values.sortBy(-_._1).saveAsTextFile(output + "-top10-berkeley")

      sc.stop
    }
    else if (switch == "tachyon") {
      val sc = new SparkContext(master, app, sparkHome, Seq(jarFile))

      var file = sc.textFile("tachyon://localhost:19999/log4j.properties")
      file take 2 mkString ("\n")
      sc.stop

    }

  }

}