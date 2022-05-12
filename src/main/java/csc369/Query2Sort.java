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

public class Query2Sort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            String[] sa = value.toString().split("\t");
            String country = sa[0];
            String urlAndCount = sa[1];
            String url = urlAndCount.split(" ")[0];
            String count = urlAndCount.split(" ")[1];
            StringBuilder sb = new StringBuilder();
            sb.append(country);
            sb.append("\t");
            sb.append(count);
            context.write(new Text(sb.toString()), new Text(url));
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            Text val1 = (Text) wc1;
            Text val2 = (Text) wc2;
            String str1 = val1.toString();
            String str2 = val2.toString();
            String[] sa1 = str1.split("\t");
            String[] sa2 = str2.split("\t");
            String country1 = sa1[0];
            Integer count1 = Integer.parseInt(sa1[1]);
            String country2 = sa2[0];
            Integer count2 = Integer.parseInt(sa2[1]);
            if (country1.compareTo(country2) == 0) {
                return -1 * count1.compareTo(count2);
            }
            return country1.compareTo(country2);
        }
    }
}
