
package com.sksamuel.elastic4s.samples

import java.io.PrintWriter
import java.util.Properties

import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchIterator
import com.sksamuel.elastic4s.http.ElasticDsl._

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.collection.mutable.Queue
import edu.stanford.nlp.ling.CoreAnnotations.{PartOfSpeechAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import com.sksamuel.avro4s.AvroSchema
import java.io.File
import com.sksamuel.avro4s.AvroOutputStream

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
    //val iterator = SearchIterator.hits(client, search("test" / "doc").matchAllQuery.keepAlive(keepAlive = "10m").size(100).sourceInclude(List("nerNorm", "nerTyp", "posLemmas"))) // returns 50 values and blocks until the iterator gets to the last element
    val iterator = SearchIterator.hits(client, search("test") query matchQuery("nerNorm", "wellness") keepAlive (keepAlive = "10m") size (100) sourceInclude (List("nerNorm", "nerTyp", "posLemmas"))) // returns 50 values and blocks until the iterator gets to the last element
    var counter: Int = 0
    var countWords = 0
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
      val cleaned2: List[List[String]] = cleaned1.mkString(" ").split("[?.!]").map(x => x.split(" ").toList.filter(!_.equals(""))).toList // ssplit

      cleaned2.foreach(x => x.foreach(x => countWords += 1))

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
    println("countWords: " + countWords)
    println("size(coOccurrences): " + coOccurrences.size)
    /*val cleanedOrderedCoOccurrences = ListMap(coOccurrences.retain((k, v) => {/*if(v.size > 500){println("groesser 500"+v.size)};if(v.size > atleastCooccurence && v.size < 300){println("groesser atleastCooccurence")};*/v.size > atleastCooccurence}).toList.sortBy {*/
    val cleanedOrderedCoOccurrences: ListMap[String, mutable.Map[String, Int]] = ListMap(coOccurrences.retain((k, v) => v.size > atleastCooccurence).toList.sortBy {
      _._1
    }: _*) // filter the coOccurrences, they have to have a least 50 different cooccurence words AND order
    println("size(cleanedOrderedCoOccurrences): " + cleanedOrderedCoOccurrences.size)

    val props: Properties = new Properties() // set properties for annotator
    props.put("annotators", "tokenize, ssplit,pos") // set properties
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props) // annotate file
    // input: one word / output: pos Tag of that word
    def getPOS(sentence: String): String = { // get POS tags per sentence
      val document: Annotation = new Annotation(sentence)
      pipeline.annotate(document) // annotate
      val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
      val back = for {
        sentence: CoreMap <- sentences
        token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
        pos: String = token.get(classOf[PartOfSpeechAnnotation])

      } yield (pos) // return List of POS tags
      back.mkString("")
    }


    val cleanedOrderedCoOccurrences2: ListMap[String, Map[String, Int]] = cleanedOrderedCoOccurrences.filter(x => {
      getPOS(x._1) == "JJ" || getPOS(x._1) == "JJR" || getPOS(x._1) == "JJS" || getPOS(x._1) == "NN" || getPOS(x._1) == "NNS" || getPOS(x._1) == "NNP" || getPOS(x._1) == "NNPS" || getPOS(x._1) == "PDT" || getPOS(x._1) == "RB" || getPOS(x._1) == "RBR" || getPOS(x._1) == "RBS" || getPOS(x._1) == "RP" || getPOS(x._1) == "VB" || getPOS(x._1) == "VBD" || getPOS(x._1) == "VBG" || getPOS(x._1) == "VBN" || getPOS(x._1) == "VBP" || getPOS(x._1) == "VBZ" || getPOS(x._1) == "VBG" || getPOS(x._1) == "VBG"
    }).map(x => (x._1, x._2.toMap))
    println("size(cleanedOrderedCoOccurrences2): " + cleanedOrderedCoOccurrences2.size)
    new PrintWriter("coOccurrences.txt") { // open new file
      cleanedOrderedCoOccurrences2.foreach(x => write(x + "\n"))
      close // close file
    }

    //case class coocMap(map: Map[String, Int])
    case class wordList(word: String, cooc: Map[String,Int])
    val schema = AvroSchema[wordList]
    val os = AvroOutputStream.data[wordList](new File("coOccurrences.avro"))

    cleanedOrderedCoOccurrences2.toMap.foreach(x => {
      //val coocMap = List.newBuilder[coocMap]
      //val pepperoni = wordList(x._1, x._2)
      os.write(Seq(wordList(x._1, x._2)))
    })
    os.flush()
    os.close()


    println("ready with CoOccurrences")
    allDistances(cleanedOrderedCoOccurrences2)
  }

  ////////////////////
  // get co-occurrences (get the cos between the wordvectors)
  ////////////////////

  def allDistances(coOccurrences: ListMap[String, Map[String, Int]]) {

    val borderForCosAngle: Double = 0.0


    new PrintWriter("cosOfAngleMatrix.txt") { // get new PrintWriter

      // coOccurrences e.g.: (administrative,Map(pay -> 1, role -> 1, disregard -> 1, but -> 1, remain -> 1, suggest -> 1, restrict -> 1, could -> 1, 's -> 1, spark -> 1, gaffe -> 1, nominal -> 1, demise -> 1, school -> 1, judge -> 1, boyle -> 1, privilege -> 1, cost -> 1, platform -> 1, staff -> 1, oshea -> 1, ; -> 1, manager -> 1, he -> 1, closure -> 1, she -> 1, failure -> 1, client -> 1, 5 -> 1, fatally -> 1, constant -> 1, day-to-day -> 1, note -> 1, until -> 1, pend -> 1, obama -> 1, not -> 1, set -> 1, nursing -> 1, of -> 1, charge -> 1, aas -> 1, director -> 1, ludicrous -> 1, function -> 1, both -> 1, take -> 1, have -> 1, diocese_of_fargo -> 1, public_opinion -> 1, include -> 1, approximately -> 1, down -> 1, you -> 1, now -> 1, teena_jibilian -> 1, / -> 1, book -> 1, some -> 1, leave -> 1, or -> 1, headquarters -> 1, relation -> 1, va -> 1, administrative -> 2, united_states -> 1, bursa -> 1, they -> 1, convenience -> 1, repository -> 1, will -> 1, chaos -> 1, base -> 1, -rrb- -> 1, shoot -> 1, use -> 1, state -> 1, be -> 1, put -> 1, broad -> 1, only -> 1, assistant -> 1, -lrb- -> 1, threat -> 1, from -> 1, dismiss -> 1, clear -> 1, datum -> 1, after -> 1, if -> 1, to -> 1, amount -> 1, url -> 1, employee -> 1, and -> 1, that -> 1, hussein_chahine -> 1, who -> 1, eradication -> 1, : -> 1, reassign -> 1, film -> 1, church -> 1, high_school -> 1, should -> 1, pps -> 1, sector -> 1, claim -> 1, daystreamNumber -> 1, confusion -> 1, number -> 1, sudden -> 1, effort -> 1, suppose -> 1, portland -> 1, for -> 1, a -> 1, allocate -> 1, involvement -> 1, fee -> 1, on -> 1, duty -> 1, chief -> 1, maintain -> 1, daystreamDate -> 1, with -> 1, legal -> 1, court -> 1, by -> 1, in -> 1, % -> 1, year -> 1, space -> 1, merely -> 1, ughelli_judicial_division -> 1, hold -> 1, future -> 1, post -> 1, deputy -> 1, '' -> 1, at -> 1, decision -> 1, since -> 1, operating -> 1, pastoral -> 1, we -> 1, stats -> 1, tom_jurich -> 1, machinery -> 1, officer -> 1, allow -> 1, train -> 1, structure -> 1, youth -> 1, these -> 1, `` -> 1, previously -> 1, bring -> 1, deportation -> 1, up -> 1, the -> 1, process -> 1, curtail -> 1, existing -> 1, service -> 1, render -> 1, so -> 1, it -> 1, feature -> 1, support -> 1, genuine -> 1, tom_rinehart -> 1, true -> 1, policy -> 1, - -> 1, practice -> 1, fun -> 1, amid -> 1, shape -> 1, hip -> 1, really -> 1, purpose -> 1, case -> 1, reach -> 1, connection-based -> 1, place -> 1, byzantine -> 1, financing -> 1, provide -> 1, online -> 1, lot -> 1, technology -> 1, two -> 1, tax -> 1, law -> 1, such -> 1, phase -> 1, federal -> 1, medical -> 1, create -> 1, reverse -> 1, unpaid -> 1, say -> 1, teacher -> 1, new -> 1, sick -> 1, accord -> 1, access -> 1, this -> 1, which -> 1, office -> 1, there -> 1, robust -> 1, sri_lanka -> 1, currently -> 1, financial -> 1, drive -> 1, -lsb- -> 1, other -> 1, athletic_director -> 1, as -> 1, under -> 1, completion -> 1, nearly -> 1, noronica -> 1, system -> 1))
      coOccurrences.foreach { case (firstWord, firstMap) => { // for each word in the map
        var cosOfAngleMatrix = scala.collection.mutable.Map[String, ListMap[String, Double]]() // we can save the distances to other word vectors here
        cosOfAngleMatrix(firstWord) = ListMap[String, Double]() // make a entry for the current word
        val lengthFirstWordVector = math.floor(scala.math.sqrt(firstMap.values.foldLeft(0.0)((x, y) => x + scala.math.pow(y, 2))) * 100) / 100 // calc the length for the current word vector
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

            val lengthSecondWordVector = math.floor(scala.math.sqrt(math.floor(secondMap.values.foldLeft(0.0)((x, y) => x + scala.math.pow(y, 2)) * 100) / 100) * 100) / 100 // length of second word vector
            val cosOfAngleFirstWordSecondWord: Double = dotProductFirstWordSecondWord / (lengthFirstWordVector * lengthSecondWordVector) // cosAngle
            if (lengthSecondWordVector > 0 && lengthSecondWordVector > 0 && cosOfAngleFirstWordSecondWord > borderForCosAngle) { // filter results
              //println("firstWord: "+firstWord+"secondWord: "+secondWord)
              val tmp: ListMap[String, Double] = cosOfAngleMatrix(firstWord).updated(secondWord, (math floor cosOfAngleFirstWordSecondWord * 1000) / 1000)
              //cosOfAngleMatrix(firstWord)(secondWord) = (math floor cosOfAngleFirstWordSecondWord * 100) / 100
              cosOfAngleMatrix(firstWord) = tmp
            } else {

            }
          }
        }
        }

        ////
        // POS ANNOTATION
        ////

        val props: Properties = new Properties() // set properties for annotator
        props.put("annotators", "tokenize, ssplit,pos") // set properties
        val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props) // annotate file
        // input: one word / output: pos Tag of that word
        def getPOS(sentence: String): String = { // get POS tags per sentence
          val document: Annotation = new Annotation(sentence)
          pipeline.annotate(document) // annotate
          val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList
          val back = for {
            sentence: CoreMap <- sentences
            token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
            pos: String = token.get(classOf[PartOfSpeechAnnotation])

          } yield (pos) // return List of POS tags
          back.mkString("")
        }

        ////
        // filter POS tags that doesn't match to main words POS tag
        ////

        cosOfAngleMatrix(firstWord) = ListMap(cosOfAngleMatrix(firstWord).toList.sortBy {
          _._2
        }.reverse: _*) // sort and get best 11 results
        val firstWordPOS: String = getPOS(firstWord)
        cosOfAngleMatrix(firstWord) = cosOfAngleMatrix(firstWord).filter(x => getPOS(x._1) == firstWordPOS).take(11)

        ////
        // calculate the relative cosine angle
        ////

        def relCosSimMatrix(cosOfAngleMap: Map[String, Double]): ListMap[String, Double] = {
          //if(cosOfAngleMap.size > 0){
          val returnValue: Map[String, Double] = (for (currentTuple <- cosOfAngleMap) yield {
            val cosineSimCurrent: Double = cosOfAngleMap(currentTuple._1)
            val sumCosineSimTop10: Double = cosOfAngleMap.reduce((tuple1, tuple2) => ("placeholder", tuple1._2 + tuple2._2))._2 - currentTuple._2
            if (sumCosineSimTop10 > 0) {
              (currentTuple._1, cosineSimCurrent / sumCosineSimTop10)
            } else {
              (currentTuple._1, 0.0)
            }
          }).filter(x => x._2 >= 0.11)
          ListMap(returnValue.toList.sortBy {
            _._2
          }.reverse: _*) // return ordered soultions
        }

        cosOfAngleMatrix(firstWord) = relCosSimMatrix(cosOfAngleMatrix(firstWord).filter(x => x._1 != firstWord))
        cosOfAngleMatrix.filter(x => x._2.size > 0).foreach(x => write(x + "\n"))
        cosOfAngleMatrix.empty

      }
      }
      close
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
