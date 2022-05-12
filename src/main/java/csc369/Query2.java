package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Query2 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = MapWritable.class;

    // Mapper for Access Log file
    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(sa[0]);
            Text url = new Text();
            url.set(sa[6]);
            MapWritable source = new MapWritable();
            source.put(new Text("source"), new Text("A"));
            source.put(new Text("url"), url);
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
    public static class JoinReducer extends  Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)  throws IOException, InterruptedException {
            Text country = new Text();
            HashMap<Text, Integer> urlCount = new HashMap<Text, Integer>();

            for (MapWritable val : values) {
                if (val.get(new Text("source")).equals(new Text("A"))) {
                    Text currentUrl = (Text) val.get(new Text("url"));
                    if (urlCount.containsKey(currentUrl)) {
                        urlCount.replace(currentUrl, urlCount.get(currentUrl) + 1);
                    } else {
                        urlCount.put(currentUrl, 1);
                    }
                } else {
                    country = (Text) (val.get(new Text("country")));
                }
            }

            for (Map.Entry<Text, Integer> entry : urlCount.entrySet()) {
                MapWritable out = new MapWritable();
                out.put(entry.getKey(), new IntWritable(entry.getValue()));
                context.write(country, out);
            }
        }
    }
}
