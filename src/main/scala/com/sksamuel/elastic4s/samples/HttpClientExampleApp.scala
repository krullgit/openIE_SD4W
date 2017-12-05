package com.sksamuel.elastic4s.samples


import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchResponse}
import com.sksamuel.elastic4s.http.ElasticDsl._



object HttpClientExampleApp extends App {
  def test() {
    println("test")
  }
  // METHODS

  // just prints the text field from the results
  def printHits(x: List[SearchHit]) {
    println("#results: " + x.size)
    x.foreach(y => println(y.sourceField(("text")).asInstanceOf[Map[Any, Any]].get("string") match {
      case Some(i) => println(i)
      case _ => ""
    }))
  }

  // prints a list(distinct) and asks to choose some
  def chooseSomething(list: List[String], topic: String): List[String] = {
    println(">ENTITY1< >TRIGGER< >ENTITY2<\n")
    val listClean = list.filter(i => !i.equals("")).distinct.filter(i => !i.equals("trigger"))
    println(s"///// $topic /////\n")
    for (i <- listClean.indices) {
      println(s"$i ${listClean(i)}")
    }
    println("")
    val y = scala.io.StdIn.readLine(s"Please choose $topic (e.g.: 1,2,3): ")
    if (y.equals("")) {
      val temp = List.range(0, listClean.size).map(x => listClean(x))
      println(s"Chosen $topic: " + temp + "\n")
      temp
    } else {
      val temp = y.split(",").toList.map(x => x.toInt).map(x => listClean(x))
      println(s"Chosen $topic: " + temp + "\n")
      temp
    }
  }

  // help funcion: to option (sometimes useful)
  def toOption(value: Any): Option[Any] = Option(value)

  // help funcion: can flatten nested Lists
  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case i: List[_] => flatten(i)
    case e => List(e)
  }

  // returns a list with lists from the response
  def getValueListofLists(response: List[SearchHit], path: String): List[List[String]] = {
    val temp = for (j <- response.indices) yield {
      getFieldFromSearchHit(response(j), path)
    }
    temp.toList //.filter(i => !i.equals("")).distinct
  }

  // returns a list with strings from the response
  def getValueList(response: List[SearchHit], path: String): List[String] = {
    val temp = for (j <- response.indices) yield {
      getFieldFromSearchHit(response(j), path)
    }
    temp.flatten.toList //.filter(i => !i.equals("")).distinct
  }

  def getValueListAndParse(response: List[SearchHit], path: String): List[String] = {
    val temp = for (j <- response.indices) yield {
      response(j).sourceField(path).toString.split(" ~ ").toList
    }
    temp.flatten.toList //.filter(i => !i.equals("")).distinct
  }

  // returns a list of values from the response
  def getFieldFromSearchHit(searchhit: SearchHit, path: String): List[String] = {
    val pathSplit:List[String] = path.split("\\.").toList
    val sourceField = pathSplit(0)
    try {
      searchhit.sourceField(sourceField)
    } catch {
      case e: Exception => return List("")
    }
    val listBuilder = List.newBuilder[String]
    val y: Any = searchhit.sourceField(sourceField)

    downThePath(1, y)

    def downThePath(count: Int, innerElement: Any) {
      innerElement.asInstanceOf[Map[Any, Any]] match {
        case i: Map[Any, Any] => i.get(pathSplit(count)) match {
          case Some(j) => j match {
            case k: List[Any] =>
              if (k.nonEmpty) {
                k.foreach(downThePath(count + 1, _))
              }
            case l: Map[Any, Any] => downThePath(count + 1, l)
            case i: String => listBuilder += i
            case _ => println("Error: no match")
          }
          case _ => println("Error: failed get")
        }
        case _ => println("Error: this is not a Map")
      }
    }
    listBuilder.result()
  }

  def getFieldFromJson(json: String, path: String): List[String] = {
    val pathSplit: List[String] = path.split("\\.").toList

    val listBuilder = List.newBuilder[String]


    downThePath(1, json)

    def downThePath(count: Int, innerElement: Any) {
      innerElement.asInstanceOf[Map[Any, Any]] match {
        case i: Map[Any, Any] => i.get(pathSplit(count)) match {
          case Some(j) => j match {
            case k: List[Any] =>
              if (k.nonEmpty) {
                k.foreach(downThePath(count + 1, _))
              }
            case l: Map[Any, Any] => downThePath(count + 1, l)
            case i: String => listBuilder += i
            case _ => println("Error: no match")
          }
          case _ => println("Error: failed get")
        }
        case _ => println("Error: this is not a Map")
      }
    }

    listBuilder.result()
  }

  // performs a http query to elastic
  def execute(Query: String = "{\"match_all\": {}}", SourceInclude: List[String]) = {
    client.execute { // get all conceptMentions (which has the relationMentions)
      search("rss" / "rss3").rawQuery(Query) sourceInclude SourceInclude size 1000 //.*Hamburg.*Berlin.*
    }.await.hits.hits.toList
  }

  def executeSd4w(client: HttpClient, Query: String = "{\"match_all\": {}}", SourceInclude: List[String] = List(""), size: Int = 10) = {
    client.execute { // get all conceptMentions (which has the relationMentions)
      search("test" / "doc").rawQuery(Query) /*sourceInclude SourceInclude*/ size size //.*Hamburg.*Berlin.*
    }.await.hits.hits.toList
  }

  // prints a list with matching suggestions
  def printSuggestions(typeList: List[List[String]], valueList: List[List[String]], chosenEntities1:List[String],chosenEntities2:List[String] ) ={

    val verbs = List("trigger")
    chosenEntities1.foreach(x => print(""+x+" "))
    print("// ")
    verbs.foreach(x => print(""+x+" "))
    print("// ")
    chosenEntities2.foreach(x => print(""+x+" "))
    print("\n\n")

    val listbuilder = List.newBuilder[String]

    for (counterTypeLists <- typeList.indices) {
      listbuilder.clear()
      for (counterType <- typeList(counterTypeLists).indices ) {
        if(chosenEntities1.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder += " // "
      for (counterType <- typeList(counterTypeLists).indices ) {
        if(verbs.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder += " // "

      for (counterType <- typeList(counterTypeLists).indices ) {
        if(chosenEntities2.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType)
        }
      }
      listbuilder.result().foreach(x => print(""+x+" "))
      println("")
    }
  }

  // prints a list with matching suggestions
  def printConceptsAndValues(typeList: List[List[String]], valueList: List[List[String]], chosenEntities1:List[String] ) ={

    val verbs = List("trigger")
    chosenEntities1.foreach(x => print(""+x+" "))
    print("// ")
    verbs.foreach(x => print(""+x+" "))
    print("\n\n")

    val listbuilder = List.newBuilder[String]

    for (counterTypeLists <- typeList.indices) {
      listbuilder.clear()
      for (counterType <- typeList(counterTypeLists).indices ) {
        if(chosenEntities1.contains(typeList(counterTypeLists)(counterType))){
          listbuilder += valueList(counterTypeLists)(counterType) + " # "
        }
      }
      if (!listbuilder.result().isEmpty){
        listbuilder.result().foreach(x => print(""+x+" "))
        println("")
      }
    }
  }

  // Asks for the topic of search and further for the boosts of every keyword
  def startAndGetBoosts(): (List[String], List[String], List[String]) = {
    println("+++++++++++++++++++++++++++++\n+++++++++++++++++++++++++++++\nTell me what you wanna crawl the web for?")
    val firstAsk = scala.io.StdIn.readLine("").split(" ").toList

    val entityList = List.newBuilder[String]

    def getAllTuples(countUp: Int) {
      def getTuple(countDown: Int) {
        var sentencePart = firstAsk(countUp)
        for (i <- 1 to countDown) {
          if (firstAsk.size > (countUp + i)) {
            sentencePart += " " + firstAsk(countUp + i)
          }
        }
        sentencePart = sentencePart.replaceAll("\\.", "")
        val stopWords: List[String] = "going I'm tell looking want know #hereTheOriginalListStarts# a about above across after afterwards again against all almost alone along already also although always am among amongst amoungst amount an and another any anyhow anyone anything anyway anywhere are around as at back be became because become becomes becoming been before beforehand behind being below beside besides between beyond bill both bottom but by call can cannot cant co computer con could couldnt cry de describe detail do done down due during each eg eight either eleven else elsewhere empty enough etc even ever every everyone everything everywhere except few fifteen fify fill find fire first five for former formerly forty found four from front full further get give go had has hasnt have he hence her here hereafter hereby herein hereupon hers herse\" him himse\" his how however hundred i ie if in inc indeed interest into is it its itse\" keep last latter latterly least less ltd made many may me meanwhile might mill mine more moreover most mostly move much must my myse\" name namely neither never nevertheless next nine no nobody none noone nor not nothing now nowhere of off often on once one only onto or other others otherwise our ours ourselves out over own part per perhaps please put rather re same see seem seemed seeming seems serious several she should show side since sincere six sixty so some somehow someone something sometime sometimes somewhere still such system take ten than that the their them themselves then thence there thereafter thereby therefore therein thereupon these they thick thin third this those though three through throughout thru thus to together too top toward towards twelve twenty two un under until up upon us very via was we well were what whatever when whence whenever where whereafter whereas whereby wherein whereupon wherever whether which while whither who whoever whole whom whose why will with within without would yet you your yours yourself yourselves".split(" ").toList
        val sentencePartWithoutStopWords: String = sentencePart.split(" ").toList.filter(x => !stopWords.contains(x.toLowerCase)).mkString(" ")

        if (sentencePartWithoutStopWords.isEmpty) return


        val query = executeSd4w(client, Query = s"""{\"match_phrase\":{\"nerNorm\":{\"query\":\"$sentencePartWithoutStopWords\"}}}""", SourceInclude = List("nerNorm"), 1);
        if (!query.isEmpty) {
          entityList += sentencePartWithoutStopWords
          if (countDown - 1 >= 0) getTuple(countDown - 1) else return
        }
        if (countDown - 1 > 0) {
          getTuple(countDown - 1)
        }
      }

      getTuple(2)
      if (entityList.result().size > 0) {
        val indexOfFirstWordInTuple = firstAsk.indexOf(entityList.result().last.split(" ").toList(0))
        if (indexOfFirstWordInTuple > countUp) {
          if (countUp + 1 < firstAsk.size) getAllTuples(indexOfFirstWordInTuple) else return
        } else {
          if (countUp + 1 < firstAsk.size) getAllTuples(countUp + 1) else return
        }
      } else {
        if (countUp + 1 < firstAsk.size) getAllTuples(countUp + 1) else return
      }
    }

    getAllTuples(0)
    println(entityList.result())
    //ask for the boosts
    val entityListSorted = entityList.result().distinct.sortWith((x, y) => x.length() > y.length())

    var entityListTemp = List.newBuilder[String]
    val exampleListCorrect = List.newBuilder[String]
    val exampleListIncorrect = List.newBuilder[String]
    //val entitySplited = i.filter(!entityListTemp.result().contains(_)).split(" ")
    for (i <- entityListSorted) {
      var entitySplited = i.split(" ")
      if (!entityListTemp.result().mkString(" ").split(" ").contains(entitySplited(0))) {
        val query = executeSd4w(client, Query = s"""{\"match_phrase\":{\"nerNorm\":{\"query\":\"$i\"}}}""", SourceInclude = List("nerNorm"), 100);
        val nerTypList = getValueListAndParse(query, "nerNorm")
        val nerNormList = getValueListAndParse(query, "nerNorm").map(x => x.toLowerCase).distinct.filter(x => !x.matches(".*" + "http" + ".*"))
        val entitySplited = i.split(" ")
        val nerExamples = for (i <- entitySplited) yield {
          nerNormList.filter(x => x.toLowerCase.matches(".*" + i.toLowerCase() + ".*")).map(x => x.toLowerCase).distinct.take(2).mkString(", ")
        }
        val ifExtractionIsCorrect = scala.io.StdIn.readLine(s"""Is the following extraction correct?(0:no,1:yes): $i (e.g.:${nerExamples.distinct.mkString(", ")}): """).toInt
        if (ifExtractionIsCorrect != 0) {
          entityListTemp += i

          def ifExampleIsCorrect(counter: Int) {
            if (nerNormList(counter).toLowerCase.matches(".*" + i.toLowerCase() + ".*")) {
              var isExampleCorrect = scala.io.StdIn.readLine(s"""Is the following example correct?(0:no,1:yes): ${nerNormList(counter)}:""").toInt
              if (isExampleCorrect == 1) {
                exampleListCorrect += nerNormList(counter)
                if ((exampleListCorrect.result().size + exampleListIncorrect.result().size) % 4 == 0) {
                  if (scala.io.StdIn.readLine("Continue?(0:no,1:yes)").equals("1")) {
                    if (counter < nerNormList.size) {
                      ifExampleIsCorrect(counter + 1)
                    }
                  }
                } else {
                  if (counter < nerNormList.size - 1) {
                    ifExampleIsCorrect(counter + 1)
                  }
                }
              } else {
                exampleListIncorrect += nerNormList(counter)
              }
            } else {
              if (counter < nerNormList.size - 1) {
                ifExampleIsCorrect(counter + 1)
              }
            }
          }

          ifExampleIsCorrect(0)
        }
      }
    }
    println(exampleListCorrect.result())
    println(exampleListIncorrect.result())
    (entityListTemp.result(), exampleListCorrect.result(), exampleListIncorrect.result())
  }

  // performs a bool query
  def queryBoolShould(entityList: List[String], entityBoosts: List[Int] = List()): String = {
    if (!entityBoosts.isEmpty) {
      val entityBoostsNormalized = entityBoosts.map(x => {
        if (x == 0) {
          0
        } else if (x == 1) {
          0.1
        } else if (x == 2) {
          0.2
        } else if (x == 3) {
          0.3
        }
      })
      val queryInner = for (i <- entityList.indices) yield {
        s"""{"match_phrase": {"nerNorm":  { "boost" : ${entityBoosts(i)},"query" : "${entityList(i)}"}}},"""
      }
      val queryInner2 = for (i <- entityList.indices) yield {
        if (entityBoosts(i) == 3) {
          s"""{"match_phrase": {"nerNorm":  { "query" : "${entityList(i)}"}}},"""
        }
      }
      if (!queryInner2.filter(x => x.equals("()")).isEmpty) {
        s"""{\"bool\": {\"should\": [${queryInner.mkString("").dropRight(1)}],\"must\": [${queryInner2.mkString("").dropRight(1)}]}}"""
      } else {
        s"""{\"bool\": {\"should\": [${queryInner.mkString("").dropRight(1)}]}}"""
      }
    } else {
      val queryInner = for (i <- entityList.indices) yield {

        s"""{"match_phrase": {"nerNorm":  { "query" : "${entityList(i)}"}}},"""
      }
      s"""{\"bool\": {\"should\": [${queryInner.mkString("").dropRight(1)}]}}"""
    }
  }
  // MAIN
  // nerNorm # nerTyp # posLemmas # post

  val client = HttpClient(ElasticsearchClientUri("localhost", 9200)) // new client
  while (true) {
    val (entityList1: List[String], exampleListCorrect1: List[String], exampleListIncorrect1: List[String]) = startAndGetBoosts
    val response1 = executeSd4w(client, queryBoolShould(entityList1), List("title.string", "text.string"), 3)
    getValueList(response1, "text.string").foreach(x => {
      println(x);
      println("#############################")
    })
  }

  // get first results


  /*
  val query1 = executeSd4w() // get all conceptMentions (which has the relationMentions)

  var nerTypList = getValueListAndParse(query1, "nerTyp")
  val chosennerTypList: List[String] = chooseSomething(nerTypList, "nerTyp")

  var nerNormList = getValueListAndParse(query1, "nerNorm")
  val choseNerNormListMust: List[String] = chooseSomething(nerNormList, "nerNorm must")
  val choseNerNormListShould: List[String] = chooseSomething(nerNormList, "nerNorm should")
  */

  /*
  var posList = getValueListAndParse(query1, "pos")
  val chosenposList: List[String] = chooseSomething(posList, "pos")
  var posLemmasList = getValueListAndParse(query1, "posLemmas")
  val chosenposLemmas: List[String] = chooseSomething(posLemmasList, "posLemmas")*/
  /*
    val iteration1TypeList = getValueListofLists(concResp, "conceptMentions.array.type")
    val iteration1ValueList = getValueListofLists(concResp, "conceptMentions.array.normalizedValue.string")

    printConceptsAndValues(iteration1TypeList,iteration1ValueList,chosenRel)
  */
  // stuff for rss hand annotated
  /*
  val relSourceInclude = List("relationMentions.array.name")
  val relResp = execute(SourceInclude = relSourceInclude) // get all conceptMentions (which has the relationMentions)
  var relList = getValueList(relResp, "relationMentions.array.name") // get all relationMentions

  val chosenRel: List[String] = chooseSomething(relList, "triggers") // choose some relationMentions
  val chosenRelAsRegex = chosenRel.mkString("|")

  val typeQuery = "{\"regexp\":{\"relationMentions.array.name.keyword\":{\"value\":\"" + chosenRelAsRegex + "\",\"boost\":1.2}}    }"
  val typeSourceInclude = List("relationMentions.array.args.array.conceptMention.type")
  val typeResp = execute(typeQuery, typeSourceInclude) // get all conceptMentions (which has the relationMentions)
  val typeList = getValueList(typeResp, "relationMentions.array.args.array.conceptMention.type") // get all relationMentions

  val chosenEntities1 = chooseSomething(typeList, "entity 1") // choose some Entities
  val chosenEntities2 = chooseSomething(typeList, "entity 2") // choose some Entities
  val chosenEntities1AsRegex = chosenEntities1.mkString("|")
  val chosenEntities2AsRegex = chosenEntities2.mkString("|")

  val iteration1Query = "{\"bool\":{\"must\":[{\"regexp\":{\"relationMentions.array.name.keyword\":\"" + chosenRelAsRegex + "\"}},{\"regexp\":{\"conceptMentions.array.type.keyword\":\"" + chosenEntities1AsRegex + "\"}},{\"regexp\":{\"conceptMentions.array.type.keyword\":\"" + chosenEntities2AsRegex + "\"}}]}}"
  val iteration1SourceInclude = List("text","relationMentions.array.args.array.role","relationMentions.array.args.array.conceptMention.type","relationMentions.array.args.array.conceptMention.normalizedValue.string")
  val iteration1Resp = execute(iteration1Query, iteration1SourceInclude) // get all conceptMentions (which has the relationMentions)

  val iteration1RoleList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.role")
  val iteration1TypeList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.conceptMention.type")
  val iteration1ValueList = getValueListofLists(iteration1Resp, "relationMentions.array.args.array.conceptMention.normalizedValue.string")

  printSuggestions(iteration1RoleList,iteration1TypeList,iteration1ValueList,chosenEntities1, chosenEntities2)
  */
  client.close()
}

