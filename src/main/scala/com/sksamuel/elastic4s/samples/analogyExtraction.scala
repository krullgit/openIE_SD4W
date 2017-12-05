
package com.sksamuel.elastic4s.samples

import java.io.PrintWriter
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchIterator
import com.sksamuel.elastic4s.http.ElasticDsl._
import scala.concurrent.duration._

object analogyExtraction {


  ////////////////////
  // get a file with all tokens (cleaned)
  ////////////////////

  def allTokens {
    val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
    implicit val timeout = Duration(10, "seconds") // is the timeout for the SearchIterator.hits method
    val listBuilder = List.newBuilder[String]

    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "1m").size(50).sourceInclude("posLemmas")) // returns 50 values and blocks until the iterator gets to the last element
    iterator.foreach(x => { // for each element in the iterator
      x.sourceField("posLemmas").toString.split(" ~ ").toList.distinct
        .filter(x => !x.matches("http" + ".*")
          && !x.matches("<a" + ".*")
          && !x.matches("www" + ".*")
          && !x.matches(".*" + ".com" + ".*")
          && !x.matches(".*" + ".org" + ".*")
          && !x.matches(".*" + ".net" + ".*")
          && !x.matches("<img" + ".*"))
        .map(x => x.toLowerCase).foreach(listBuilder += _) // filter result (www, http, <a)
    })

    new PrintWriter("allTokens.txt") { // open new file
      listBuilder.result().distinct.sorted.foreach(x => write(x + "\n")) // write distinct list du file
      close // close file
    }
    client.close() // close HttpClient
  }

  ////////////////////
  // get a file with all named entities (cleaned)
  ////////////////////

  def allNamedEntities() {
    val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
    implicit val timeout = Duration(10, "seconds") // is the timeout for the SearchIterator.hits method
    val listBuilder = List.newBuilder[String]

    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "1m").size(50).sourceInclude("nerNorm")) // returns 50 values and blocks until the iterator gets to the last element
    iterator.foreach(x => { // for each element in the iterator
      x.sourceField("nerNorm").toString.split(" ~ ").toList.distinct

        .map(x => x.toLowerCase).foreach(listBuilder += _) // filter result (www, http, <a)
    })

    new PrintWriter("allNamedEntities.txt") { // open new file
      listBuilder.result().distinct.sorted.foreach(x => write(x + "\n")) // write distinct list du file
      close // close file
    }
    client.close() // close HttpClient
  }

  ////////////////////
  // END
  ////////////////////

  def main(args: Array[String]): Unit = {
    allNamedEntities
  }
}
