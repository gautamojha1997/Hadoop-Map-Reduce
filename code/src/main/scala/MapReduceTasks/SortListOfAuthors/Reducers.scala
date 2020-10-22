package MapReduceTasks.SortListOfAuthors

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}


import scala.jdk.CollectionConverters._

//import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Reducers extends Reducer[Text, Text, Text, IntWritable]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing reducer from the input obtained from CoAuthorCountMapper")
    val authors = new ListBuffer[String]
    //val author_set = new mutable.HashSet[String]()
    values.forEach(v=>authors += v.toString)
    context.write(key, new IntWritable(authors.size))
    logger.info("Reducer execution completed")
  }
}

class PublicationCountReducer extends Reducer[Text, IntWritable, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Executing reducer to display total count of input from mapper completed")

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    context.write(key, new IntWritable(sum))
  }
}
