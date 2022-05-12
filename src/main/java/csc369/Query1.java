package csc369;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Query1 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = MapWritable.class;

    // Mapper for Access Log file
    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(sa[0]);
            MapWritable source = new MapWritable();
            source.put(new Text("source"), new Text("A"));
            context.write(hostname, source);
        }
    }

    // Mapper for Host Country csv file
    public static class HostCountryMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split(",");
            if (text.length == 2) {
                String hostname = text[0];
                String country = text[1];
                MapWritable out = new MapWritable();
                out.put(new Text("source"), new Text("B"));
                out.put(new Text("country"), new Text(country));
                context.write(new Text(hostname), out);
            }
        }
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, MapWritable, IntWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)  throws IOException, InterruptedException {
            Text country = new Text();
            Integer count = 0;

            for (MapWritable val : values) {
                if (val.get(new Text("source")).equals(new Text("A"))) {
                    count += 1;
                } else {
                     country = (Text) (val.get(new Text("country")));
                }
            }
            context.write(new IntWritable(count), country);
        }
    }
}

