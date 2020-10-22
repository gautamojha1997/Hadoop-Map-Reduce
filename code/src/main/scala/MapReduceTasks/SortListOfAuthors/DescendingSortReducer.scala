package MapReduceTasks.SortListOfAuthors

import java.lang

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.Reducer

/*This Scala file consists of two classes one just to write the key value for top 100
and the other is custom comparator for descending order sort*/


class DescendingSortReducer extends Reducer[IntWritable, Text, Text, IntWritable]{
  var count = 0
  override def reduce(key: IntWritable, values: lang.Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      //emit till count is less than 100
      values.forEach(v =>
        if(count<100){
          context.write(v,key)
          count += 1
      }
      )

  }
}


//Custom Comparator class to sort in descending order.
class SortComparator extends WritableComparator(classOf[IntWritable], true){

  override def compare(a: WritableComparable[_], b: WritableComparable[_]): Int = {
    val k1 = a.asInstanceOf[IntWritable]
    val k2 = b.asInstanceOf[IntWritable]

    -1 * k1.compareTo(k2) // - denotes sorting in desc order
  }

}
