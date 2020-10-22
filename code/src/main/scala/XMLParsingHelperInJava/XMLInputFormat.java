package XMLParsingHelperInJava;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 External library to manage map reduce input format for xml input type
 Works for multiple input tags
 The input format is based on Apache mahout and Referred to example at https://github.com/Mohammed-siddiq/hadoop-XMLInputFormatWithMultipleTags
 */

public class XMLInputFormat extends TextInputFormat {

    public static final String START_TAG_KEYS = "xmlinput.start";
    public static final String END_TAG_KEYS = "xmlinput.end";


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] startTags;
        private byte[][] endTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();


        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;

            String[] sTags = tac.getConfiguration().get(START_TAG_KEYS).split(",");

            String[] eTags = tac.getConfiguration().get(END_TAG_KEYS).split(",");
            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];
            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);

            }
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);


        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {

                int res = findMatchingTag(startTags, false);
                if (res != -1) { // Read until start_tag1 or start_tag2
                    try {

                        buffer.write(startTags[res - 1]);
                        //Changed to read all the contents before the end tag
                        int res1 = findMatchingTag(endTags, true);
                        if (res1 != -1) {
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }


        //For every byte value of input stream of the input file
        //Method checks every incoming byte value against the corresponding tag byte values and returns the
        //index of matched tag
        private int findMatchingTag(byte[][] match, boolean withinBlock) throws IOException {
            int article = 0, inproceedings = 0, proceedings = 0, book = 0, incollection = 0, phdthesis = 0, mastersthesis = 0, www = 0, person = 0, data = 0;
            while (true) {
                int b = fsin.read();

                //case when we reach the end of the file
                if (b == -1) return -1;

                if (withinBlock) buffer.write(b);


                if (b == match[0][article]) {
                    article++;
                    if (article >= match[0].length) return 1;
                } else article = 0;

                if (b == match[1][inproceedings]) {
                    inproceedings++;
                    if (inproceedings >= match[1].length) return 2;
                } else inproceedings = 0;

                if (b == match[2][proceedings]) {
                    proceedings++;
                    if (proceedings >= match[2].length) return 3;
                } else proceedings = 0;

                if (b == match[3][book]) {
                    book++;
                    if (book >= match[3].length) return 4;
                } else book = 0;

                if (b == match[4][incollection]) {
                    incollection++;
                    if (incollection >= match[4].length) return 5;
                } else incollection = 0;

                if (b == match[5][phdthesis]) {
                    phdthesis++;
                    if (phdthesis >= match[5].length) return 6;
                } else phdthesis = 0;

                if (b == match[6][mastersthesis]) {
                    mastersthesis++;
                    if (mastersthesis >= match[6].length) return 7;
                } else mastersthesis = 0;

                if (b == match[7][www]) {
                    www++;
                    if (www >= match[7].length) return 8;
                } else www = 0;

                if (b == match[8][person]) {
                    person++;
                    if (person >= match[8].length) return 9;
                } else person = 0;

                if (b == match[9][data]) {
                    data++;
                    if (data >= match[6].length) return 10;
                } else data = 0;


                if (!withinBlock && (article == 0 && inproceedings == 0 && proceedings == 0 && book == 0 && incollection == 0 && phdthesis == 0 && mastersthesis == 0 && www ==0 && person == 0 && data == 0) && fsin.getPos() >= end)
                    return -1;
            }
        }

    }


}