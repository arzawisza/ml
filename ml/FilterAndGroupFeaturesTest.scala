package com.hackhaton.krk

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.collection.mutable

class FilterAndGroupFeaturesTest extends FlatSpec with BeforeAndAfterEach {
  var context: StreamingContext = _
  val ipr = """.*(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}).*""".r

  override def beforeEach(): Unit = {
    //context = new StreamingContext("local[2]", "App", Seconds(2))
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
