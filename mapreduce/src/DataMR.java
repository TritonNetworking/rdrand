

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataMR {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			System.err.println("Usage arguments: [input directory] [output directory] [record length]");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, Integer.parseInt(args[2]));
		conf.setInt("mapreduce.job.split.metainfo.maxsize", -1);
		
		Job job = Job.getInstance(conf, "randomness");
		job.setJarByClass(DataMR.class);
		
		job.setMapperClass(DataMapper.class);
		job.setReducerClass(DataReducer.class);
		
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.submit();
		
	}

}
