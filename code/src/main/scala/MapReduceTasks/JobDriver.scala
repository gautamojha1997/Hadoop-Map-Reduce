package MapReduceTasks


import MapReduceTasks.PubsWithMostAuthorForEachVenue.{PubWithMostAuthorMapper, PubWithMostAuthorReducer}
import MapReduceTasks.PubsWithOneAuthorForEachVenue.{PubWithOneAuthorMapper, PubWithOneAuthorReducer}
import MapReduceTasks.SortListOfAuthors.{CoAuthorCountMapper, DescendingSortMapper, DescendingSortReducer, PublicationCountMapper, PublicationCountReducer, Reducers, SortComparator}
import MapReduceTasks.TopTenAuthorsAtEachVenue.{TopTenAuthorReducers, TopTenAuthorsMapper}
import XMLParsingHelperInJava.XMLInputFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object JobDriver {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    val conf = ConfigFactory.load("Config")

    configuration.set("mapred.textoutputformat.separator", ",")
    //loading start tags
    configuration.set("xmlinput.start", conf.getString("start-tags"))

    //loading end_tags
    configuration.set("xmlinput.end", conf.getString("end-tags"))
    configuration.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization")

    //Sort list of Authors with most numbers of Co-Authors
    logger.info("Starting first job to calculate count of co-authors");{
    val job1 = Job.getInstance(configuration, "Calculates number of co-authors")
    job1.setJarByClass(this.getClass)
    val ip1 = new Path(args(1))
    val op1 = new Path(args(2) +  conf.getString("CoAuthorCountOutputPath"))
    //Mapper Class
    job1.setMapperClass(classOf[CoAuthorCountMapper])
    job1.setInputFormatClass(classOf[XMLInputFormat])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[Text])
    //Reducer Class
    job1.setReducerClass(classOf[Reducers])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job1, ip1)
    FileOutputFormat.setOutputPath(job1, op1)
    job1.waitForCompletion(true)


    val job2 = Job.getInstance(configuration, "Sorts Count of co-authors in desc order")
    job2.setJarByClass(this.getClass)
    //val ip2 = new Path(args(1))
    val op2 = new Path(args(2) +  conf.getString("SortedMostCoAuthorPath"))
    //Mapper Class
    job2.setMapperClass(classOf[DescendingSortMapper])
    //job2.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job2.setMapOutputKeyClass(classOf[IntWritable])
    job2.setMapOutputValueClass(classOf[Text])
    //Reducer Class
    job2.setReducerClass(classOf[DescendingSortReducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setSortComparatorClass(classOf[SortComparator])
    FileInputFormat.addInputPath(job2, op1)
    FileOutputFormat.setOutputPath(job2, op2)
    job2.waitForCompletion(true)
    }


    //Sort list of Authors with no co-authors
    logger.info("Starting job that sorts the authors with no co-author");{
      val job3 = Job.getInstance(configuration, "Calculates number of publications for each author")
      job3.setJarByClass(this.getClass)
      val ip3 = new Path(args(1))
      val op3 = new Path(args(2) +  conf.getString("PublicationCountPath"))
      //Mapper Class
      job3.setMapperClass(classOf[PublicationCountMapper])
      job3.setInputFormatClass(classOf[XMLInputFormat])
      job3.setMapOutputKeyClass(classOf[Text])
      job3.setMapOutputValueClass(classOf[IntWritable])
      //Reducer Class
      job3.setReducerClass(classOf[PublicationCountReducer])
      job3.setOutputKeyClass(classOf[Text])
      job3.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job3, ip3)
      FileOutputFormat.setOutputPath(job3, op3)
      job3.waitForCompletion(true)


      val job4 = Job.getInstance(configuration, "Sorts Count of Publications for each Co-author in desc order")
      job4.setJarByClass(this.getClass)
      //val ip2 = new Path(args(1))
      val op4 = new Path(args(2) +  conf.getString("SortedWithoutCoAuthorPath"))
      //Mapper Class
      job4.setMapperClass(classOf[DescendingSortMapper])
      //job2.setInputFormatClass(classOf[KeyValueTextInputFormat])
      job4.setMapOutputKeyClass(classOf[IntWritable])
      job4.setMapOutputValueClass(classOf[Text])
      //Reducer Class
      job4.setReducerClass(classOf[DescendingSortReducer])
      job4.setOutputKeyClass(classOf[Text])
      job4.setOutputValueClass(classOf[IntWritable])
      job4.setSortComparatorClass(classOf[SortComparator])
      FileInputFormat.addInputPath(job4, op3)
      FileOutputFormat.setOutputPath(job4, op4)
      job4.waitForCompletion(true)
    }

    //Venues with list of publications having only one author
    logger.info("Starting job which produces list of publications that contains only one author");{
      val job5 = Job.getInstance(configuration, "Produces list of publications that contains only one author")
      job5.setJarByClass(this.getClass)
      val ip5 = new Path(args(0))
      val op5 = new Path(args(1) +  conf.getString("VenueWithListOfPubsWithOneAuthor"))
      //Mapper Class
      job5.setMapperClass(classOf[PubWithOneAuthorMapper])
      job5.setInputFormatClass(classOf[XMLInputFormat])
      job5.setMapOutputKeyClass(classOf[Text])
      job5.setMapOutputValueClass(classOf[Text])
      //Reducer Class
      job5.setReducerClass(classOf[PubWithOneAuthorReducer])
      job5.setOutputKeyClass(classOf[Text])
      job5.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job5, ip5)
      FileOutputFormat.setOutputPath(job5, op5)
      job5.waitForCompletion(true)
    }


    //Venues with list of pubs with most authors
    logger.info("Starting job which produces list of publications that contains only one author");
    {
      val job6 = Job.getInstance(configuration, "Produces list of publications that contains most author")
      job6.setJarByClass(this.getClass)
      val ip6 = new Path(args(1))
      val op6 = new Path(args(2) + conf.getString("VenueWithListOfPubsWithMostAuthor"))
      //Mapper Class
      job6.setMapperClass(classOf[PubWithMostAuthorMapper])
      job6.setInputFormatClass(classOf[XMLInputFormat])
      job6.setMapOutputKeyClass(classOf[Text])
      job6.setMapOutputValueClass(classOf[Text])
      //Reducer Class
      job6.setReducerClass(classOf[PubWithMostAuthorReducer])
      job6.setOutputKeyClass(classOf[Text])
      job6.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job6, ip6)
      FileOutputFormat.setOutputPath(job6, op6)
      job6.waitForCompletion(true)
    }

    //Job for Top10Authors
    logger.info("Starting job which produces list of Top 10 Authors At each venue");
    {
      val job7 = Job.getInstance(configuration, "Produces list of Top 10 Authors At each venue")
      job7.setJarByClass(this.getClass)
      val ip7 = new Path(args(1))
      val op7 = new Path(args(2)+ conf.getString("VenueWithListOfTopTenAuthor"))
      //Mapper Class
      job7.setMapperClass(classOf[TopTenAuthorsMapper])
      job7.setInputFormatClass(classOf[XMLInputFormat])
      job7.setMapOutputKeyClass(classOf[Text])
      job7.setMapOutputValueClass(classOf[Text])
      //Reducer Class
      job7.setReducerClass(classOf[TopTenAuthorReducers])
      job7.setOutputKeyClass(classOf[Text])
      job7.setOutputValueClass(classOf[Text])
      FileInputFormat.addInputPath(job7, ip7)
      FileOutputFormat.setOutputPath(job7, op7)
      job7.waitForCompletion(true)
    }

  }


}
