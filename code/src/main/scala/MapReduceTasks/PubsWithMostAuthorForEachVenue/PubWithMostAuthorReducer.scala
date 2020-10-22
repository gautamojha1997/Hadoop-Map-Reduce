package MapReduceTasks.PubsWithMostAuthorForEachVenue

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class   PubWithMostAuthorReducer extends Reducer[Text, Text, Text, Text]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Reducer combines and reduces the venue and publication title with most number of authors.")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    //TreeMap with author count as value and title as value
    try {
      val tree_map: mutable.TreeMap[Int, String] = new mutable.TreeMap[Int, String]()(Ordering[Int].reverse)

      val pattern = """thead:(.+):>CntAuth:(\d+)""".r
      values.forEach(entry => {
        val pattern(t, a) = entry.toString
        val k = (a).toInt;
        val v = (t);
        if (tree_map.contains(k)) tree_map.put(k, tree_map(k) + "|" + v)
        else tree_map.put(k, v)
        //val first_value_treemap = tree_map(tree_map.firstKey)
      })
      context.write(key, new Text(tree_map(tree_map.firstKey)))
    }catch {
      case matchError: MatchError => logger.error(matchError.getMessage())
    }
  }
}
