package MapReduceTasks.PubsWithOneAuthorForEachVenue

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters.IterableHasAsScala
/*This class emits publications with one author for each venues*/
class PubWithOneAuthorReducer extends Reducer[Text, Text, Text, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Reducer combines and reduces the venue and publication title")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {

    val sum = values.asScala.foldLeft("")((a,b) => a + b + "->")
    context.write(key, new Text(sum))
  }

}
