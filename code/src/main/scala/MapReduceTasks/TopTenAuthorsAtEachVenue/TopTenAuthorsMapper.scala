package MapReduceTasks.TopTenAuthorsAtEachVenue

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML
/*This class emits Venue and author for that venue to the mapper*/
class TopTenAuthorsMapper extends Mapper[LongWritable, Text, Text, Text]{

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
    logger.info("Mapper which returns Venue and Author")
    val StringXml =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"

    val author_list = retrieveElementFromXml(StringXml,"author")
    val journal = retrieveElementFromXml(StringXml,"journal")
    val booktitle = retrieveElementFromXml(StringXml,"booktitle")
    val publisher = retrieveElementFromXml(StringXml,"publisher")

    if(author_list.nonEmpty){
      if(journal.nonEmpty){
        author_list.foreach(a =>context.write(new Text(journal.head),new Text(a)))
      }
      if(booktitle.nonEmpty){
        author_list.foreach(a =>context.write(new Text(booktitle.head),new Text(a)))
      }
      if(publisher.nonEmpty){
        author_list.foreach(a =>context.write(new Text(publisher.head),new Text(a)))
      }
    }
  }

}
