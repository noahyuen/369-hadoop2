package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("Query1".equalsIgnoreCase(otherArgs[0])) {

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, Query1.AccessLogMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, Query1.HostCountryMapper.class);

		job.setReducerClass(Query1.JoinReducer.class);

		job.setOutputKeyClass(Query1.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("Query1Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query1Sort.ReducerImpl.class);
		job.setMapperClass(Query1Sort.MapperImpl.class);
		job.setOutputKeyClass(Query1Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1Sort.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Query1Sort2".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(Query1Sort2.MapperImpl.class);
		job.setSortComparatorClass(Query1Sort2.SortComparator.class);
		job.setOutputKeyClass(Query1Sort2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1Sort2.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("Query2".equalsIgnoreCase(otherArgs[0])) {

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, Query2.AccessLogMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, Query2.HostCountryMapper.class);

		job.setReducerClass(Query2.JoinReducer.class);

		job.setOutputKeyClass(Query2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query2.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("Query2Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(Query2Sort.MapperImpl.class);
		job.setSortComparatorClass(Query2Sort.SortComparator.class);
		job.setOutputKeyClass(Query2Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query2Sort.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
