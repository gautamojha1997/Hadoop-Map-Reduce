package MapReduceTasks.PubsWithMostAuthorForEachVenue

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class PubWithMostAuthorMapper extends Mapper[LongWritable, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val conf = ConfigFactory.load("Config")

  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  def retrieveElementFromXml(xml: String, ele: String): List[String] = {
    logger.info("Retrieving Specified Element list from XML")
    logger.info(xml + ": " + ele)
    val parent = XML.loadString(xml)
    val list_element = (parent \\ ele).map(el => el.text.toLowerCase.trim).toList
    list_element
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Mapper which returns Venue and Title with only most author")
    val StringXml =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"

    val author_list = retrieveElementFromXml(StringXml,"author")
    val editor_list = retrieveElementFromXml(StringXml,"editor")

    if (author_list.nonEmpty){
      val title = retrieveElementFromXml(StringXml,"title")
      val size = author_list.size

      val journal = retrieveElementFromXml(StringXml,"journal")
      if(journal.nonEmpty){
        context.write(new Text(journal.head), new Text("thead:" + title.head + ":>" +"CntAuth:"+ size))
      }
      val booktitle = retrieveElementFromXml(StringXml,"booktitle")
      if(booktitle.nonEmpty){
        context.write(new Text(booktitle.head), new Text("thead:" + title.head + ":>" +"CntAuth:"+ size))
      }
      val publisher = retrieveElementFromXml(StringXml,"publisher")
      if(publisher.nonEmpty){
        context.write(new Text(publisher.head), new Text("thead:" + title.head + ":>" +"CntAuth:"+ size))
      }
    }
  }


}
