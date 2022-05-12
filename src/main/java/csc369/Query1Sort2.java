package csc369;

import java.text.DateFormatSymbols;
import java.io.IOException;
import java.time.Month;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Query1Sort2 {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            String[] sa = value.toString().split("\t");
            Text country = new Text();
            IntWritable count = new IntWritable();
            country.set(sa[1]);
            count.set(Integer.parseInt(sa[0]));
            context.write(count, country);
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            IntWritable val1 = (IntWritable) wc1;
            IntWritable val2 = (IntWritable) wc2;
            return -1 * val1.compareTo(val2);
        }
    }
}
