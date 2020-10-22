package MapReduceTasks.PubsWithMostAuthorForEachVenue

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
/*This class produces list of publications with most author for each venue*/
class   PubWithMostAuthorReducer extends Reducer[Text, Text, Text, Text]{

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Reducer combines and reduces the venue and publication title with most number of authors.")

  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    //TreeMap with author count as value and title as value
    try {
      //Tree map which stores key as count and values as publication ordered in reverse to produce the top most values.
      val tree_map: mutable.TreeMap[Int, String] = new mutable.TreeMap[Int, String]()(Ordering[Int].reverse)

      //handling bad input through regex
      val pattern = """thead:(.+):>CntAuth:(\d+)""".r
      values.forEach(entry => {
        val pattern(t, a) = entry.toString
        val k = (a).toInt; //splitting input from pattern
        val v = (t); //splitting from pattern
        if (tree_map.contains(k)) tree_map.put(k, tree_map(k) + "|" + v) // if the tree already has the key just append the value to the key
        else tree_map.put(k, v) //otherwise put key and value
        //val first_value_treemap = tree_map(tree_map.firstKey)
      })
      context.write(key, new Text(tree_map(tree_map.firstKey)))
    }catch {
      case matchError: MatchError => logger.error(matchError.getMessage())
    }
  }
}
