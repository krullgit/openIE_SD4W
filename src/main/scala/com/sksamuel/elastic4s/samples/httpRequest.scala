/*<dependency>
  <groupId>org.scalaj</groupId>
  <artifactId>scalaj-http_2.12</artifactId>
  <version>2.3.0</version>
</dependency>
  <dependency>
    <groupId>com.typesafe.play</groupId>
    <artifactId>play-json_2.12</artifactId>
    <version>2.6.7</version>
  </dependency>

package com.sksamuel.elastic4s.samples

import scalaj.http._
import play.api.libs.json._
import scala.util.{Try, Success, Failure}


object request {

  def main(args: Array[String]) {
    while (true) {
      val word: String = scala.io.StdIn.readLine("").toString
      val response: HttpResponse[String] = Http(s"""https://od-api.oxforddictionaries.com/api/v1/inflections/en/$word""")
        .headers(Map("app_id" -> "efc38cac", "app_key" -> "72068e0d67c6bb789e0fd6dd93cf9f1c")).asString

      def getLemma(body: String): String = {
        for (i <- 0 until 3) {
          val problem = Try((Json.parse(body)) ("results")(0)("lexicalEntries")(i)("grammaticalFeatures"))

          problem match {
            case Success(v) =>
              if ((Json.parse(body)) ("results")(0)("lexicalEntries")(i)("inflectionOf")(0)("text").toString().drop(1).dropRight(1) != word) {
                return (Json.parse(body)) ("results")(0)("lexicalEntries")(i)("inflectionOf")(0)("text").toString().drop(1).dropRight(1)
              }
            case Failure(e) =>
            //println(e.getMessage)
          }
        }
        ""
      }

      val lemma = getLemma(response.body)
      println(lemma)

    }
  }
}
*/