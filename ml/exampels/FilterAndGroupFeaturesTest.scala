package com.azawisza.logs

import java.io.{File, PrintWriter}

import com.sundogsoftware.sparkstreaming.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable

class FilterAndGroupFeaturesTest extends FlatSpec with BeforeAndAfterEach {
  var context: StreamingContext = _
  val ipr = """.*(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}).*""".r

  override def beforeEach(): Unit = {
    context = new StreamingContext("local[2]", "App", Seconds(2))
  }


  "LFI Feature"should "be recognized" in {

  }

  "Features" should "be grouped by vurnerability using spark" in {
    val queue = mutable.Queue[RDD[String]]() //fake queue of RDDs

    val d = new File("src/test/resources/logs")
    val files = d.listFiles().filter(_.isFile).filter(!_.getName.contains("error"))
    files.map(_.getAbsolutePath).map(scala.io.Source.fromFile).foreach( e => {
      val string = e.mkString
      //strings+=string
      println("ADDing log ,,")

    })
    import nl.basjes.parse.core.Casts
    import nl.basjes.parse.core.Parser

    import nl.basjes.parse.core.Parser
    //108.238.100.85 - - [20/Jan/2015:07:40:21 -0800] "GET /honeypot/Honeypot%20-%20Howto.pdf HTTP/1.1" 206 65874 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:35.0) Gecko/20100101 Firefox/35.0"
    import nl.basjes.parse.httpdlog.HttpdLoglineParser
    //val logformat = "%t %u [%D %h %{True-Client-IP}i %{UNIQUE_ID}e %r] %{Cookie}i %s \"%{User-Agent}i\" \"%{host}i\" %l %b %{Referer}i"
    //104.130.20.53 - - [22/Jan/2015:03:41:12 -0800] "GET /honeypot/BSidesDFW HTTP/1.1" 301 460 "-" "Tornado-Async-Client"
    val logformat = "%h %l %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""
    val dummyParser = new HttpdLoglineParser(classOf[Record], logformat)

    //dummyParser.addTypeRemapping("request.firstline.uri.query.g", "HTTP.URI", Casts.STRING_ONLY)
    //dummyParser.addTypeRemapping("request.firstline.uri.query.r", "HTTP.URI", Casts.STRING_ONLY)

    queue += context.sparkContext.makeRDD(Seq("")) //adding sequence with json text to queue
    val dstream:InputDStream[String] = context.queueStream(queue) //creating dstream
    dstream
      .foreachRDD((rdd, time)=> {
        rdd
          .flatMap(e=>{
            println("LOGS:---->"+e.length)
            e.lines.toList
          }).foreach(line=>{

          val record:Record = dummyParser.parse(line)
          println(record)

        })

      })
    context.start()
    context.awaitTermination()
  }


  "Features" should "be grouped by vulnerability detected by scalp" in {

    val queue = mutable.Queue[RDD[String]]()
    val stream: InputDStream[String] = context.queueStream(queue)

    aggregateLinesWithErrorsOf("csrf")
    aggregateLinesWithErrorsOf("dos")
    aggregateLinesWithErrorsOf("dt")
    aggregateLinesWithErrorsOf("id")
    aggregateLinesWithErrorsOf("lfi")
    aggregateLinesWithErrorsOf("ref")
    aggregateLinesWithErrorsOf("spam")
    aggregateLinesWithErrorsOf("sqli")
    aggregateLinesWithErrorsOf("xss")
    aggregateLinesWithErrorsOf("sqli")
  }


  "Fetures " should "be labeled and stored" in {
    val vus: List[File] = readFilesOf("vulnerabilities")
    val allVus = vus.filter(_.isFile)
      .map(f => {
        scala.io.Source.fromFile(f).mkString
      }).flatMap(_.lines)
      .map(_.trim())
      .toSet
    var i = 0
    var lc = 0
    var f = 0
    val file: File = new File("alllabeled-0.txt")
    var pw = new PrintWriter(file)

    println("vus found=" + allVus.size)
    val files: List[File] = readFilesOf("csrf")
    files.filter(_.isFile)
      .filter(_.getName.contains("access"))
      .map(f => {
        scala.io.Source.fromFile(f).mkString
      }).flatMap(_.lines)
      .map(_.trim)
      .map {
        case line if allVus.contains(line) => {
          i = i + 1
          line + " \"1\"\n"
        }
        case line => line + " \"0\"\n";
      }.foreach(e => {
      println(e)
      pw.write(e)
      pw.flush()
      lc = lc + 1
      if (lc > 30000){
        pw.close()
        f=f+1
        val file: File = new File(s"alllabeled-$f.txt")
        pw = new PrintWriter(file)
        lc = 0
      }
    })

    pw.close()
    println("vul matched=" + i)
  }

  //test

  def aggregateLinesWithErrorsOf(vu: String) = {
    val file: File = new File(s"$vu.txt")
    if (file.exists()) {
      file.delete()
    }
    val pw = new PrintWriter(file)
    val files: List[File] = readFilesOf(vu)
    files.filter(_.isFile)
      .filter(_.getName.contains("_scalp_"))
      .map(f => {
        scala.io.Source.fromFile(f).mkString
      }).flatMap(_.lines)
      .filter(ipr.findFirstIn(_).isDefined)
      .map(_.trim())
      .foreach(line => pw.write(line + System.lineSeparator()))
    pw.flush()
    pw.close()
  }

  def readFilesOf(vu: String): List[File] = {
    val d = new File(s"src/test/resources/data/$vu")
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List()
    }
  }

  override def afterEach(): Unit = {
    context.stop()
  }
}
