import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;  

public class FreqItems {

	public static int supportTreshold;
	public static int confidenceTreshold;
	
	public static void main(String[] args) throws Exception {
		
		Scanner reader = new Scanner(System.in);
		System.out.println("Enter a number as the support treshold: ");
		supportTreshold = reader.nextInt();
		System.out.println("Enter a number as the confidence treshold: ");
		confidenceTreshold = reader.nextInt();
		reader.close();
		
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path("C:/Users/steff/git/MapReduce_apriori_SV_VR/out"), true);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Freq items <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "freq items");
		job.setJarByClass(FreqItems.class);
		job.setMapperClass(basketMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(itemReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

