package com.azawisza.logs

import com.sundogsoftware.sparkstreaming.Record
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

class PrepareParserTest extends FlatSpec with BeforeAndAfterEach {

  "Parser"should "be prepared " in {
    val s = "58.218.200.115 - - [07/Nov/2017:21:18:47 -0800] \"GET //NewsType.asp?SmallClass=%27%20union%20select%200,username%2BCHR(124)%2Bpassword,2,3,4,5,6,7,8,9%20from%20admin%20union%20select%20*%20from%20news%20where%201=2%20and%20%27%27=%27 HTTP/1.1\" 404 239 \"-\" \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)\""
    val logformat = "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\""

    val dummyParser = new HttpdLoglineParser(classOf[Record], logformat)
    val record = dummyParser.parse(s)
    println(record)

  }
}
