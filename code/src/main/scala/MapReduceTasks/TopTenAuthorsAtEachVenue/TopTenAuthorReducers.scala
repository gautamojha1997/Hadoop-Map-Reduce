package MapReduceTasks.TopTenAuthorsAtEachVenue

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

/*This Class emits the final output of top ten author at each venue*/

class TopTenAuthorReducers extends Reducer[Text, Text, Text, Text]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Reducer combines and reduces top ten authors at each venue")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val authors_list = values.map(v=>v.toString).toList //taking list of authors obtained for each venue
    if(authors_list.nonEmpty){
      val topTenAuthors = authors_list.groupBy(identity).toList.sortBy(-_._2.size).take(10) // grouping the author with same identity and sorting by second tuple and taking top 10.
      val topTenStrings = topTenAuthors.map{case (k,v) => s"$k" } //mapping the top 10 authors
      if(topTenAuthors.nonEmpty){
        context.write(key, new Text(topTenStrings.mkString(";")))
      }
    }

  }

}
