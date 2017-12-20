
package com.sksamuel.elastic4s.samples

import java.io.PrintWriter

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchIterator
import com.sksamuel.elastic4s.http.ElasticDsl._

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.collection.mutable.Queue

object analogyExtraction {


  ////////////////////
  // get a file with all tokens (cleaned)
  ////////////////////

  def allTokens {
    val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
    implicit val timeout = Duration(10, "seconds") // is the timeout for the SearchIterator.hits method
    val listBuilder = List.newBuilder[String]

    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "1m").size(100).sourceInclude("posLemmas")) // returns 50 values and blocks until the iterator gets to the last element
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
  // get a file with all named entity types (cleaned)
  ////////////////////

  def allNerTyps() {
    val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
    implicit val timeout = Duration(10, "seconds") // is the timeout for the SearchIterator.hits method
    val listBuilder = List.newBuilder[String]

    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "1m").size(50).sourceInclude("nerTyp")) // returns 50 values and blocks until the iterator gets to the last element
    iterator.foreach(x => { // for each element in the iterator
      x.sourceField("nerTyp").toString.split(" ~ ").toList.distinct

        .map(x => x.toLowerCase).foreach(listBuilder += _) // filter result (www, http, <a)
    })

    new PrintWriter("allNerTyps.txt") { // open new file
      listBuilder.result().distinct.sorted.foreach(x => write(x + "\n")) // write distinct list du file
      close // close file
    }
    client.close() // close HttpClient
  }


  ////////////////////
  // get a Map with all named entities and their transformations (e.g. coca cola -> coca_cola OR 07.05.1987 -> daystreamDate)
  ////////////////////

  def allNamedEntitiesTransformation(implicit client: HttpClient): scala.collection.mutable.Map[String, String] = {

    implicit val timeout = Duration(1000, "seconds") // is the timeout for the SearchIterator.hits method
    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "10m").size(100).sourceInclude(List("nerNorm", "nerTyp"))) // returns 50 values and blocks until the iterator gets to the last element
    var allNamedEntitiesTransformationsOutput = scala.collection.mutable.Map[String, String]()

    new PrintWriter("allNamedEntitiesTransformations.txt") {
      //var allNamedEntitiesTransformations = scala.collection.mutable.Map[String, String]()
      iterator.foreach(x => { // for each element in the iterator
        val NerNorms = List.newBuilder[String]
        val NerTypes = List.newBuilder[String]
        x.sourceField("nerNorm").toString.split(" ~ ").toList
          .map(x => x.toLowerCase)
          .foreach(x => NerNorms += x)
        x.sourceField("nerTyp").toString.split(" ~ ").toList
          .map(x => x.toLowerCase)
          .foreach(x => NerTypes += x)
        val NerNormsResults = NerNorms.result()
        val NerTypesResults = NerTypes.result()
        if (NerNormsResults.size == NerTypesResults.size) {
          for (i <- NerNormsResults.indices) {
            if (NerTypesResults(i).equals("date")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamDate"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamDate"
            } else if (NerTypesResults(i).equals("distance")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamDistance"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamDistance"
            } else if (NerTypesResults(i).equals("duration")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamDuration"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamDuration"
            } else if (NerTypesResults(i).equals("money")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamMoney"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamMoney"
            } else if (NerTypesResults(i).equals("number")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamNumber"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamNumber"
            } else if (NerTypesResults(i).equals("percent")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamPercent"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamPercent"
            } else if (NerTypesResults(i).equals("time")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamTime"
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamTime"
            } else if (NerTypesResults(i).equals("url")) {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = "daystreamUrl"
              //allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamUrl"
            } else {
              //allNamedEntitiesTransformations(NerNormsResults(i)) = NerNormsResults(i).replaceAll(" ", "_")
              allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = NerNormsResults(i).replaceAll(" ", "_")
            }
          }
        }
      })
      allNamedEntitiesTransformationsOutput.foreach(x => write(x + "\n")) // write distinct list du file
      close // close file
    }
    allNamedEntitiesTransformationsOutput
  }

  def namedEntitiesTransformation(NerNorms: List[String], NerTypes: List[String]): scala.collection.mutable.Map[String, String] = {

    var NamedEntitiesTransformationsOutput = scala.collection.mutable.Map[String, String]()

    new PrintWriter("allNamedEntitiesTransformations_small.txt") {
      //var allNamedEntitiesTransformations = scala.collection.mutable.Map[String, String]()
      if (NerNorms.size == NerTypes.size) {
        NerNorms.zip(NerTypes).zipWithIndex.map(x => (x._1._1, x._1._2, x._2)).foreach(triple => { // for each element in the iterator (NerNorm, NerType, index)

          if (triple._2.equals("date")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamDate"
          } else if (triple._2.equals("distance")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamDistance"
          } else if (triple._2.equals("duration")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamDuration"
          } else if (triple._2.equals("money")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamMoney"
          } else if (triple._2.equals("number")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamNumber"
          } else if (triple._2.equals("percent")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamPercent"
          } else if (triple._2.equals("time")) {
            NamedEntitiesTransformationsOutput(triple._1) = "daystreamTime"
          } else if (triple._2.equals("url")) {
            //allNamedEntitiesTransformationsOutput(NerNormsResults(i)) = "daystreamUrl"
          } else {
            NamedEntitiesTransformationsOutput(triple._1) = triple._1.replaceAll(" ", "_")
          }

        })
      } else {
        println("MISMATCH: NerNorms.size = " + NerNorms.size + " & NerTypes.size = " + NerTypes.size)
      }
      //allNamedEntitiesTransformationsOutput.foreach(x => write(x + "\n")) // write distinct list du file
      close // close file
    }
    NamedEntitiesTransformationsOutput
  }

  ////////////////////
  // get co-occurrences (cleaned: filter url's and replace entities with their transformation)
  ////////////////////


  def allCoOccurrences(implicit client: HttpClient) {

    implicit val timeout = Duration(1000, "seconds") // is the timeout for the SearchIterator.hits method
    val windowWidth: Int = 9
    val atleastCooccurence = 50

    var coOccurrences = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Int]]()
    val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "10m").size(100).sourceInclude(List("nerNorm", "nerTyp", "posLemmas"))) // returns 50 values and blocks until the iterator gets to the last element
    var counter: Int = 0
    iterator.foreach(searchhit => { // for each element in the iterator

      /// print
      println(counter);
      counter += 1
      ///

      val cleaned0 = searchhit.sourceField("posLemmas").toString.split(" ~ ").toList

        .filter(x => !x.matches("http" + ".*")
          && !x.matches("<a" + ".*")
          && !x.matches("www" + ".*")
          && !x.matches(".*" + ".com" + ".*")
          && !x.matches(".*" + ".org" + ".*")
          && !x.matches(".*" + ".net" + ".*")
          && !x.matches("<img" + ".*")
          && !x.matches("http" + ".*"))
        .map(x => x.toLowerCase) // filter result (www, http, <a)

      val NerNorms: List[String] = searchhit.sourceField("nerNorm").toString.split(" ~ ").toList.map(x => x.toLowerCase)
      val NerTypes: List[String] = searchhit.sourceField("nerTyp").toString.split(" ~ ").toList.map(x => x.toLowerCase)
      val namedEntitiesTransformations: mutable.Map[String, String] = namedEntitiesTransformation(NerNorms, NerTypes)

      val cleaned1 = namedEntitiesTransformations.foldLeft(cleaned0.mkString(" "))((a, b) => a.replaceAllLiterally(" " + b._1 + " ", " " + b._2 + " ")).split(" ").toList
      val cleaned2: List[List[String]] = cleaned1.mkString(" ").split("[?.!]").map(x => x.split(" ").toList.filter(!_.equals(""))).toList

      cleaned2.foreach(sentence => {
        val appending = (0 to windowWidth / 2).map(x => "imunimportant").toList
        val enlargedSentence = appending ::: sentence ::: appending

        enlargedSentence
          .sliding(windowWidth) // create sliding windows
          .foreach(window => {
          // for each window
          var centerElement = window((windowWidth / 2)) // get the middle element in the window
          coOccurrences.get(centerElement) match { // is the current middle element present in the coOccurrences matrix
            case Some(centerElementWordMap) => { // if yes // centerElementWordMap = list of words in the Map of the current middleWord
              window.foreach(wordInWindow => { // for every word y in the window
                centerElementWordMap.get(wordInWindow) match { // test if word(key) is already in the map
                  case Some(wordInWindowValue) => { // if the word is in the map
                    if (centerElement != wordInWindow && centerElement != "imunimportant" && wordInWindow != "imunimportant" && centerElement != "," && wordInWindow != ",") {
                      coOccurrences(centerElement)(centerElement) = wordInWindowValue + 1 // assign updated cooccurence count
                    }
                  }
                  case None => {
                  }
                    if (centerElement != wordInWindow && centerElement != "imunimportant" && wordInWindow != "imunimportant" && centerElement != "," && wordInWindow != ",") {
                      coOccurrences(centerElement)(wordInWindow) = 1
                    }
                }
              })
            }
            case None => { // word not in Matrix

              coOccurrences(centerElement) = collection.mutable.Map[String, Int]()
              window.foreach(wordInWindow => { // for every word y in the window
                if (centerElement != wordInWindow && centerElement != "imunimportant" && wordInWindow != "imunimportant" && centerElement != "," && wordInWindow != ",") {
                  coOccurrences(centerElement)(wordInWindow) = 1 // set wordcount the centerElement
                }
              })
            }
          }
        })
      }
      )
    })

    val cleanedOrderedCoOccurrences = ListMap(coOccurrences.retain((k, v) => v.size > atleastCooccurence).toList.sortBy {
      _._1
    }: _*) // filter the coOccurrences, they have to have a least 50 different cooccurence words AND order
    new PrintWriter("coOccurrences.txt") { // open new file
      cleanedOrderedCoOccurrences.foreach(x => write(x + "\n"))
      close // close file
    }

    println("ready with CoOccurrences")
    allDistances(cleanedOrderedCoOccurrences)
  }

  ////////////////////
  // get co-occurrences (get the cos between the wordvectors)
  ////////////////////

  def allDistances(coOccurrences: ListMap[String, mutable.Map[String, Int]]) {

    val borderForCosAngle: Double = 0.3


    new PrintWriter("cosOfAngleMatrix.txt") { // get new PrintWriter


      coOccurrences.foreach { case (firstWord, firstMap) => { // for each word in the map
        var cosOfAngleMatrix = scala.collection.mutable.Map[String, scala.collection.mutable.ListMap[String, Double]]() // we can save the distances to other word vectors here
        cosOfAngleMatrix(firstWord) = collection.mutable.ListMap[String, Double]() // make a entry for the current word
        val lengthFirstWordVector = scala.math.sqrt(firstMap.values.foldLeft(0.0)((x, y) => x + scala.math.pow(y, 2))) // calc the length for the current word vector
        coOccurrences.foreach { case (secondWord, secondMap) => { // get the seconds word for comparison
          var dotProductFirstWordSecondWord: Int = 0 // initiate the dotproduct
          secondMap.foreach { case (wordInSecondMap, countInSecondMap) => { // get every word in the second word
            firstMap.get(wordInSecondMap) match { // and look if this words are present in the first word
              case Some(countInFirstMap) => {
                dotProductFirstWordSecondWord += countInFirstMap * countInSecondMap // if both words occur in both word vectors calculate the product
              }
              case None => // this case is not interesting
            }
          }
          }
          if (dotProductFirstWordSecondWord > 0) {

            val lengthSecondWordVector = scala.math.sqrt(secondMap.values.foldLeft(0.0)((x, y) => x + scala.math.pow(y, 2))) // length of second word vector
            val cosOfAngleFirstWordSecondWord: Double = dotProductFirstWordSecondWord / (lengthFirstWordVector * lengthSecondWordVector) // cosAngle
            if (lengthSecondWordVector > 0 && lengthSecondWordVector > 0 && cosOfAngleFirstWordSecondWord > borderForCosAngle) { // filter results
              cosOfAngleMatrix(firstWord)(secondWord) = (math floor cosOfAngleFirstWordSecondWord * 100) / 100
            } else {

            }
          }
        }
        }
        cosOfAngleMatrix(firstWord) = mutable.ListMap(cosOfAngleMatrix(firstWord).toList.sortBy {
          _._2
        }.reverse: _*)
        cosOfAngleMatrix.filter(x => x._2.size > 1).foreach(x => write(x + "\n"))
        cosOfAngleMatrix.empty
      }
      }
    }
  }


  ////////////////////
  // END
  ////////////////////

  def main(args: Array[String]): Unit = {
    implicit val timeout = Duration(1000, "seconds") // is the timeout for the SearchIterator.hits method
    implicit val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
    allCoOccurrences
    client.close() // close HttpClient
  }
}
