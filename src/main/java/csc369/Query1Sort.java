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

public class Query1Sort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            String[] sa = value.toString().split("\t");
            Text country = new Text();
            IntWritable count = new IntWritable();
            country.set(sa[1]);
            count.set(Integer.parseInt(sa[0]));
            context.write(country, count);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, IntWritable, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text country, Iterable<IntWritable> counts,
                              Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = counts.iterator();

            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(result, country);
        }
    }
}