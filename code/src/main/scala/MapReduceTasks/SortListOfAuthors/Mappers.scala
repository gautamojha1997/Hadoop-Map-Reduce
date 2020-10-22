package MapReduceTasks.SortListOfAuthors

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}


import scala.xml.XML
/*
Mappers consists of two mapper class for producing Top 100 Authors as per specified*/
class Mappers {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def retrieveAuthorFromXml(xml: String, ele: String): List[String] = {
    logger.info("Retrieving Authors list from XML")
    logger.info(xml + ": " + ele)
    val parent = XML.loadString(xml)
    val authorsList = (parent \\ ele).map(el => el.text.toLowerCase.trim).toList
    authorsList
  }
}

//This class emits Author with it's co-author count to the reducer.
class CoAuthorCountMapper extends Mapper[LongWritable, Text, Text, Text] {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val conf = ConfigFactory.load("Config")

    val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      logger.info("Executing mapper to get count of co-author for every co-author")
      val StringXml =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
      val authors_list = new Mappers().retrieveAuthorFromXml(StringXml,"author") //extract list of authors from the XML string
      logger.info("Map author with co-author")
      if(authors_list.nonEmpty){
        authors_list.foreach(a => authors_list.foreach(i => if (a!=i){context.write(new Text(a),new Text(i))}))
      }
      logger.info("CoAuthorCountMapper execution completed")
    }
  }

//This class emits Author with count one to the reducer.
class PublicationCountMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val conf = ConfigFactory.load("Config")
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing mapper to get publication count by each author")
    val StringXml =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors_list = new Mappers().retrieveAuthorFromXml(StringXml,"author") //extract list of authors from the XML string
    if (authors_list.size == 1){
      authors_list.foreach(a => context.write(new Text(a), new IntWritable(1)))
    }
    logger.info("PublicationCountMapper execution completed")
  }
}










