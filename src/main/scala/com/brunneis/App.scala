/*
    Polypus: a Big Data Self-Deployable Architecture for Microblogging
    Text Extraction and Real-Time Sentiment Analysis

    Copyright (C) 2017 Rodrigo Martínez (brunneis) <dev@brunneis.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.brunneis

import java.sql.{DriverManager, PreparedStatement, Timestamp}

import it.nerdammer.spark.hbase._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer

/**
  * @author brunneis
  */

object App {

  // 32 buckets
  private final val BUCKETS = Seq(
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21",
    "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
  )

  def main(args: Array[String]) {

    // Distributed methods
    object SerializedMethods extends java.io.Serializable {
      def getWindow(post_timestamp: Long, separators: List[Long]): Int = {

        return separators.indexOf(separators.filter(_ >= post_timestamp).min)
      }

      def getRelevance(nick: String): Double = {
        return 1D
      }

      def normalizeString(originalString: String, isKeyword: Boolean = false): String = {

        // Arrays for normalization
        val accentedLetters = "áàäéèëíìïóòöúùuñÁÀÄÉÈËÍÌÏÓÒÖÚÙÜÑçÇ"
        val unAccentedLetters = "aaaeeeiiiooouuunAAAEEEIIIOOOUUUNcC"
        val lowercaseLetters = "abcdefghijklmnñopqrstuvwxyz"
        val uppercaseLetters = "ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"

        var stringToModify: String = originalString

        // Remove accent marks
        var counter = 0
        while (counter < accentedLetters.length) {
          stringToModify = stringToModify.replace(accentedLetters.charAt(counter), unAccentedLetters.charAt(counter))
          counter += 1
        }

        // To lowercase
        counter = 0
        while (counter < uppercaseLetters.length) {
          stringToModify = stringToModify.replace(uppercaseLetters.charAt(counter), lowercaseLetters.charAt(counter))
          counter += 1
        }

        // Remove URLs
        stringToModify = stringToModify.replaceAll("(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?", " ")

        // Remove punctuation symbols
        stringToModify = stringToModify.replaceAll("[\\.,/:;)(#?¿!¡']", " ")

        // Remove other non alphanumeric symbols
        if(!isKeyword)
          stringToModify = stringToModify.replaceAll("[^a-z0-9_ ]", " ")

        // Remove extra spaces
        stringToModify = stringToModify.replaceAll("\\s+", " ")

        // Remove spaces at the beginning and at the end of the string
        stringToModify = stringToModify.trim()

        return stringToModify
      }

    }

    trait Parameters {
      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("unnamed"), shortName = Array("i"))
      def job_id: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("."), shortName = Array("q"))
      def search_query: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("en"), shortName = Array("l"))
      def language: String

      // Will be the stop_timestamp (inverted order in HBase)
      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("1500000000000"), shortName = Array("b"))
      def start_timestamp: Long

      // Will be the start_timestamp (inverted order in HBase)
      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("1600000000000"), shortName = Array("e"))
      def stop_timestamp: Long

      // Window size in millis
      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("900000"), shortName = Array("w"))
      def window_size: Long

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("untagged"), shortName = Array("t"))
      def job_tag: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("localhost"), shortName = Array("z"))
      def zookeeper_quorum: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("posts"), shortName = Array("n"))
      def hbase_table: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("polypus_primary"), shortName = Array("c"))
      def hbase_cf: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("localhost"), shortName = Array("m"))
      def mariadb_host: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("dpsa"), shortName = Array("d"))
      def mariadb_database: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("dpsa"), shortName = Array("u"))
      def mariadb_user: String

      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("dpsa"), shortName = Array("p"))
      def mariadb_password: String

      // Exec mode, 1 for APL processing
      @com.lexicalscope.jewel.cli.Option(defaultValue = Array("0"), shortName = Array("e"))
      def exec_mode: Integer
    }

    var params: Parameters = null
    try {
      params = com.lexicalscope.jewel.cli.CliFactory.parseArguments(classOf[Parameters], args: _*)
    } catch {
      case ex: com.lexicalscope.jewel.cli.ArgumentValidationException => {
        println("Invalid arguments. Exiting...")
        ex.printStackTrace()
        System.exit(1)
      }
    }

    val job_id = params job_id
    val search_query = SerializedMethods.normalizeString(params search_query, true)
    val language = params language
    var start_timestamp = params start_timestamp
    var stop_timestamp = params stop_timestamp
    val window_size = params window_size
    val job_tag = params job_tag
    val zookeeper_quorum = params zookeeper_quorum
    val hbase_table = params hbase_table
    val hbase_cf = params hbase_cf
    val mariadb_host = params mariadb_host
    val mariadb_database = params mariadb_database
    val mariadb_user = params mariadb_user
    val mariadb_password = params mariadb_password
    val apl_processing = (params exec_mode) == 1

    println("job_id = " + job_id)
    println("search_query = " + search_query)
    println("language = " + language)
    println("start_timestamp = " + start_timestamp)
    println("stop_timestamp = " + stop_timestamp)
    println("window_size = " + window_size)
    println("job_tag = " + job_tag)
    println("zookeeper_quorum = " + zookeeper_quorum)
    println("hbase_table = " + hbase_table)
    println("hbase_cf = " + hbase_cf)
    println("mariadb_host = " + mariadb_host)
    println("mariadb_database = " + mariadb_database)
    println("mariadb_user = " + mariadb_user)
    println("mariadb_password = " + mariadb_password)
    println("apl_processing = " + apl_processing)

    // Spark configuration
    val sparkConf = (new SparkConf()
      .setAppName("dpsa-spark_" + job_id)
      .set("spark.hbase.host", zookeeper_quorum)
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.rdd.compress", "true"))

    // sparkConf.setMaster(parameters spark_master)
    val sc = new SparkContext(sparkConf)

    var apl_terms = Nil: List[String]
    if (apl_processing){
      println("EXEC_MODE_APL")

      // MariaDB connection
      var conn = DriverManager.getConnection(
        "jdbc:mysql://" + mariadb_host + ":3306/" + mariadb_database,
        mariadb_user,
        mariadb_password
      )

      // Get last stop timestamp
      var ps = conn.prepareStatement("SELECT MAX(stop_timestamp) as stop_timestamp FROM dpsa_lpa_results")
      var rs = ps.executeQuery()

      // If no result the start_timestamp parameter is used as
      if (rs.next()) {
        val aux = rs.getTimestamp("stop_timestamp")
        // Take the next millisecond as start point
        if (aux != null) {
          start_timestamp = aux.getTime + 1
        }
      }
      println(start_timestamp)

      // Take as the stop point the current time
      stop_timestamp = System.currentTimeMillis()

      // Retrieve the APL keywords
      ps = conn.prepareStatement("SELECT term FROM dpsa_lpa_terms")
      rs = ps.executeQuery()
      var buffer = new ListBuffer[String]()
      while (rs.next()) {
        val term = SerializedMethods.normalizeString(rs.getString("term"))
        println("LPA -> " + term)
        buffer += term
      }

      // Sort keywords
      apl_terms = buffer.toList.sorted

      conn.close()
    }

    var filtered_posts: RDD[(Option[String], Option[Long], Option[Int], Option[String])] = null
    // ON-DEMAND
    var separators: List[Long] = Nil
    if (!apl_processing) {

      // Number of windows
      val windows: Long = Math.ceil((stop_timestamp - start_timestamp) / window_size.toFloat).toLong

      // Last separator
      val lastSeparator: Long = start_timestamp + windows * window_size

      // Separator list
      separators = List.range((start_timestamp + window_size), (lastSeparator + window_size), window_size)
      println(separators)

      // Match pattern
      val MatchingPost = new Regex("""[\s:,;.?!]*""" + search_query + """[\s:,;.?!]*""")

      filtered_posts = sc.hbaseTable[(Option[String], Option[Long], Option[Int], Option[String])](hbase_table)
        .select("content", "post_timestamp", "sentiment", "language")
        .inColumnFamily(hbase_cf)
        .withStartRow(getStartRowkey(stop_timestamp)) // Inverted order
        .withStopRow(getStopRowkey(start_timestamp))
        .withSalting(BUCKETS).filter { post =>
        // Avoid posts without content
        post._1.nonEmpty &&
          // Avoid posts without polarity (_3)
          post._3.nonEmpty &&
          // The language must match
          post._4.nonEmpty && post._4.get.equals(language) &&
          // Post timestamp in range
          post._2.nonEmpty && post._2.get >= (start_timestamp) && post._2.get <= (stop_timestamp) &&
          // The post matches the query
          MatchingPost.findFirstIn(SerializedMethods.normalizeString(post._1.get)).isDefined
      }
    }
    // APL
    else {
      filtered_posts = sc.hbaseTable[(Option[String], Option[Long], Option[Int], Option[String])](hbase_table)
        .select("content", "post_timestamp", "sentiment", "language")
        .inColumnFamily(hbase_cf)
        .withStartRow(getStartRowkey(stop_timestamp)) // Inverted order
        .withStopRow(getStopRowkey(start_timestamp))
        .withSalting(BUCKETS).filter { post =>
        var normalizedContent: String = null
        if (post._1.nonEmpty) {
          // Normalized post content
          normalizedContent = SerializedMethods.normalizeString(post._1.get)
        }

        // Avoid posts without content
        post._1.nonEmpty &&
        // Avoid posts without polarity (_3)
        post._3.nonEmpty &&
        // The language must match
        post._4.nonEmpty && post._4.get.equals(language) &&
        // Post timestamp in range
        post._2.nonEmpty &&
        // The post matches any keyword
        post._2.get >= (start_timestamp) && post._2.get <= (stop_timestamp) && (
          apl_terms.map(term => " " + term + " ").exists(normalizedContent.contains) ||
          apl_terms.map(term => term + " ").exists(normalizedContent.startsWith) ||
          apl_terms.map(term => " " + term).exists(normalizedContent.endsWith)
        )
      }
    }

    var result_ondemand: RDD[(Long, (Int, Int, Int, Int))] = null
    var result_apl: Array[(String, Double)] = null

    // ON-DEMAND
    if(!apl_processing) {
      result_ondemand = filtered_posts.map { post =>
        val windowIndex = SerializedMethods.getWindow(post._2.get, separators)
        val windowTimestamp = separators(windowIndex)

        var isPositive = 0
        if (post._3.get > 0) isPositive = 1

        var isNegative = 0
        if (post._3.get < 0) isNegative = 1

        // Separator -> (polarity, match_incrementer, positive_incrementer, negative_incrementer)
        windowTimestamp -> (post._3.get, 1, isPositive, isNegative)
        // For evey pair of K-V, for the same key (timestamp), the number of posts and the polarity
        // are aggregated. Ascending order (true). Only the aggregated windows are sorted
      }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)).sortByKey(true)
      result_ondemand.cache()
    }
    // APL
    else {
      result_apl = filtered_posts.flatMap(
        post => {
          // Normalized post content
          val normalizedContent = SerializedMethods.normalizeString(post._1.get)

          // Keywords in the post content
          val termsInside = apl_terms.filter(term => normalizedContent.contains(" " + term + " "))
          // Keywords at the beginning of the post content
          val termsBegin = apl_terms.filter(term => normalizedContent.startsWith(term + " "))
          // Keywords at the end of the post content
          val termsEnd = apl_terms.filter(term => normalizedContent.endsWith(" " + term))

          // Concatenate lists
          var termsToEmit = termsInside ::: termsBegin ::: termsEnd
          // Remove repeated terms
          termsToEmit = termsToEmit.distinct

          var buffer = new ListBuffer[(String, Double)]()
          termsToEmit.foreach { term =>
            buffer += ((term + "_result", post._3.get))
            buffer += ((term + "_posts", 1))
            buffer += ((term + "_relevance", 1))
          }

          // Emit the full sequence
          buffer.toIndexedSeq
        }
      ).reduceByKey(_ + _).sortByKey(true).collect()
    }


    // MariaDB connection
    val conn = DriverManager.getConnection(
      "jdbc:mysql://" + mariadb_host + ":3306/" + mariadb_database,
      mariadb_user,
      mariadb_password
    )

    var ps: PreparedStatement = null

    // ON-DEMAND
    if(!apl_processing) {
      // Aggregated polarity value
      val polaritySum = result_ondemand.map { case (key, value) => value._1 }.sum
      // Total matches
      val totalMatches = result_ondemand.map { case (key, value) => value._2 }.sum

      // Positive matches
      val posMatches = result_ondemand.map { case (key, value) => value._3 }.sum
      // Negative matches
      val negMatches = result_ondemand.map { case (key, value) => value._4 }.sum
      // Polarized matches
      val polarizedMatches = posMatches + negMatches
      // Neutral matches
      val neutralMatches = totalMatches - polarizedMatches

      // Avg. polarity [0, 1]
      var normalizedPolarity = (polaritySum.toFloat / totalMatches + 1) / 2
      if (normalizedPolarity.isNaN) normalizedPolarity = 0.
      // Positivity ratio
      var posPolRatio = posMatches.toFloat / polarizedMatches
      if (posPolRatio.isNaN) posPolRatio = 0.
      // Negativity ratio
      var negPolRatio = negMatches.toFloat / polarizedMatches
      if(negPolRatio.isNaN) negPolRatio = 0.

      // JSON
      var json: String = "{\"global\":{" +
        "\"polarity\":" + normalizedPolarity +
        ",\"matches\":" + totalMatches +
        ",\"pol_matches\":" + polarizedMatches +
        ",\"neutral_matches\":" + neutralMatches +
        ",\"pos_matches\":" + posMatches +
        ",\"pos_pol_ratio\":" + posPolRatio +
        ",\"neg_matches\":" + negMatches +
        ",\"neg_pol_ratio\":" + negPolRatio +
        "},\"windows\":{"

      // Partial results
      result_ondemand.collect().foreach {
        case (key, value) =>
          val windowPolMatches = value._3 + value._4

          var polarity = (value._1.toFloat / value._2 + 1) / 2
          if (polarity.isNaN) polarity = 0.toFloat

          var pos_pol_ratio = value._3.toFloat / windowPolMatches
          if (pos_pol_ratio.isNaN) pos_pol_ratio = 0.toFloat

          var neg_pol_ratio = value._4.toFloat / windowPolMatches
          if (neg_pol_ratio.isNaN) neg_pol_ratio = 0.toFloat

          json += "\"" + key + "\":{" +
            "\"polarity\":" + polarity +
            ",\"matches\":" + value._2 +
            ",\"pol_matches\":" + windowPolMatches +
            ",\"neutral_matches\":" + (value._2 - windowPolMatches) +
            ",\"pos_matches\":" + value._3 +
            ",\"pos_pol_ratio\":" + pos_pol_ratio +
            ",\"neg_matches\":" + value._4 +
            ",\"neg_pol_ratio\":" + neg_pol_ratio +
            "},"

      }
      json = json.substring(0, json.length - 1) + "}}"

      ps = conn.prepareStatement("INSERT INTO dpsa_tagged_results " +
        "(job_id, search_query, start_timestamp, stop_timestamp, result, job_tag) " +
        "VALUES (?,?,?,?,?,?)"
      )
      ps.setString(1, job_id)
      ps.setString(2, search_query)
      ps.setTimestamp(3, new Timestamp(start_timestamp))
      ps.setTimestamp(4, new Timestamp(stop_timestamp))
      ps.setString(5, json)
      ps.setString(6, job_tag)
      ps.executeUpdate

      // Update frequencies for the search query
      ps = conn.prepareStatement("INSERT INTO dpsa_tagged_freq " +
        "(term) VALUES (?) " +
        "ON DUPLICATE KEY UPDATE times = times + 1"
      )
      ps.setString(1, search_query)
      ps.executeUpdate()
    }

    // APL
    else {
      val resultValuesBuffer = new ListBuffer[Double]()
      val postsValuesBuffer = new ListBuffer[Double]()
      val relevanceValuesBuffer = new ListBuffer[Double]()
      var foundTermsBuffer = new ListBuffer[String]()

      // Found terms
      result_apl.foreach { row =>
        apl_terms.foreach { term =>
          if (row._1.startsWith(term + "_") && row._1.endsWith("_result")) {
            resultValuesBuffer += row._2
            if (!foundTermsBuffer.contains(term))
              foundTermsBuffer += term
          } else if (row._1.startsWith(term + "_") && row._1.endsWith("_posts")) {
            postsValuesBuffer += row._2
            if (!foundTermsBuffer.contains(term))
              foundTermsBuffer += term
          } else if (row._1.startsWith(term + "_") && row._1.endsWith("_relevance")) {
            relevanceValuesBuffer += row._2
            if (!foundTermsBuffer.contains(term))
              foundTermsBuffer += term
          }
        }
      }
      foundTermsBuffer = foundTermsBuffer.sorted

      ps = conn.prepareStatement("INSERT INTO " +
        "dpsa_lpa_results " +
        "(term, start_timestamp, stop_timestamp, processed_posts, total_relevance, result) " +
        "VALUES (?,?,?,?,?,?)"
      )

      foundTermsBuffer.zipWithIndex.foreach { case (term, index) =>

        val sentiment = ((resultValuesBuffer(index) / relevanceValuesBuffer(index)) + 1) / 2
        val posts = postsValuesBuffer(index)
        val relevance = relevanceValuesBuffer(index) // Number of posts (for now)

        ps.setString(1, term)
        ps.setTimestamp(2, new Timestamp(start_timestamp))
        ps.setTimestamp(3, new Timestamp(stop_timestamp))
        ps.setLong(4, posts.toLong)
        ps.setDouble(5, relevance)
        ps.setDouble(6, sentiment)
        ps.executeUpdate

      }
    }

    conn.close()
    sc.stop()

  }

  def getStartRowkey(ts2: Long): String = {
    return getReverseTimestamp(ts2)
  }

  def getStopRowkey(ts1: Long): String = {
    return getReverseTimestamp(ts1)
  }

  def getReverseTimestamp(ts: Long): String = {
    var reverseTimestamp: String = (Long.MaxValue - ts).toString
    // Number of needed zeros to match the MAX_LONG length
    val padding: Int = Long.MaxValue.toString.length - reverseTimestamp.length
    1 to padding foreach { _ => reverseTimestamp = 0 + reverseTimestamp }
    return reverseTimestamp
  }
}
