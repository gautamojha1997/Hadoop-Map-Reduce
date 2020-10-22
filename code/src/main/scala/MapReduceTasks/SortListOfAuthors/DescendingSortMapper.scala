package MapReduceTasks.SortListOfAuthors

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper


/*This class swaps the key value pairs for sorting and emits it to reducer*/
class DescendingSortMapper extends Mapper[LongWritable, Text, IntWritable, Text] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
    val text = value.toString.split(",")
    context.write(new IntWritable(text(1).toInt),new Text(text(0)))
  }
}
