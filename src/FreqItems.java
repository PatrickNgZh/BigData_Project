import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
@SuppressWarnings("unused")

public class FreqItems {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("./out"), true);
		fs.delete(new Path("./tmp"), true);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Freq items <in> <out> <tmp>");
			System.exit(2);
		}
		conf.set("in", args[0]);
		conf.set("out", args[1]);
		conf.set("tmp", args[2]);
		
		int [] tresholds = readTresholds();	
		conf.setInt("supportTreshold", tresholds[0]);
		conf.setFloat("confidenceTreshold", tresholds[1]);
		
		// add some kind of filter that empty files or files without frequent singles can be processed
		
		int run = 1;
		boolean MoreFrequents = true;
		while (MoreFrequents)
		{
			runJob(conf,run);
			MoreFrequents = generateCandidates(conf, run);
			run+=1;
		}
		
		
		System.exit(0);
		
	}

	private static boolean generateCandidates(Configuration conf, int run) throws Exception {
		boolean frequents_found = false;
		String filename = "/"+String.valueOf(run)+"/"+String.valueOf(run)+"-r-00000";
		Path pathInput=new Path(conf.get("out") +  filename);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader read=new BufferedReader(new InputStreamReader(fs.open(pathInput)));
		
		String line;
		ArrayList<String> inputList = new ArrayList <String>();
		
		
		while( (line = read.readLine()) != null) {
		    String [] items = line.split("\\t");
	    	inputList.add(items[0]);
		}
		read.close();
		
		Path pathOutput=new Path(conf.get("tmp") + filename );
		Writer write = new BufferedWriter(new OutputStreamWriter(fs.create(pathOutput)));
		
		if (run == 1){
			for (int i=0;i<inputList.size();i++){
				for (int y=i+1;y<inputList.size();y++){
					if (i==0 && y==i+1){
						write.write(inputList.get(i)+","+inputList.get(y));
					}else{
						write.write("\n"+inputList.get(i)+","+inputList.get(y));
					}
				}
			}
			frequents_found=true;
		}else{
			for (int i=0;i<inputList.size();i++){
				for (int y=i+1;y<inputList.size();y++){
					boolean match = true;
					String[] first = inputList.get(i).split(",");
					String[] second = inputList.get(y).split(",");
					String [] candidate = new String[first.length+1];
					for (int x = 0;x<first.length-1;x++){
						if (second[x]==first[x]){
							match = true;
						}
						else match =false;
					}
					if (match==true){
						
						for (int a=0;a<candidate.length-1;a++){
							candidate[a]=first[a];
						}
						candidate[candidate.length-1]=first[second.length-1];
					}
					
					if (allSubSetsFrequent(candidate,run, conf)){
						String candidateString = String.join(",",candidate);
						write.write(candidateString);
						frequents_found=true;
					}
				}
			}
		}
		
		write.close();	
		return frequents_found;
	}

	private static boolean allSubSetsFrequent(String[] candidate, int run, Configuration conf) throws Exception {
		boolean allFrequent = true;
		String filename = "/"+String.valueOf(run)+"/"+String.valueOf(run)+"-r-00000";
		FileSystem fs = FileSystem.get(conf);
		String[]allSubsets = new String [run-1];
		ArrayList<String> inputList = new ArrayList <String>();
		
		for (int i=0,j=candidate.length-3;i<candidate.length-2;i++, j--){
				String toCheck1 = String.join(",",Arrays.copyOfRange(candidate, 0, j));
				String toCheck2 = String.join(",",Arrays.copyOfRange(candidate, j+1,candidate.length-1));
				if (toCheck1 == null ||toCheck1 == ""){
					allSubsets[i]=toCheck2;
				}
				else if(toCheck2 == null ||toCheck2 == ""){
					allSubsets[i]=toCheck1;
				}
				else{
					allSubsets[i]=toCheck1+","+toCheck2;
				}
		}
		
		for (int i = 0; i<run;i++){
			Path pathInput=new Path(conf.get("tmp") +  filename);
			BufferedReader read=new BufferedReader(new InputStreamReader(fs.open(pathInput)));
			String line;
			while( (line = read.readLine()) != null) {
			    String [] items = line.split("\\t");
		    	inputList.add(items[0]);		
			}
			read.close();
		}
		
		for (int i=0 ;i<allSubsets.length;i++){
			if (!inputList.contains(allSubsets[i]))allFrequent=false;
		}
		return allFrequent;
	}

	
	private static void runJob (Configuration conf, int run) throws Exception
	{	
		
		conf.setInt("run", run);
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Run no.: "+run);
		job.setJarByClass(FreqItems.class);
		if (run==1){
			job.setMapperClass(basketMapper_1.class);
		}else job.setMapperClass(basketMapper_k.class);
		
		job.setCombinerClass(itemCombiner.class);
		job.setReducerClass(itemReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, String.valueOf(run), TextOutputFormat.class,IntWritable.class, Text.class);
		FileInputFormat.addInputPath(job, new Path(conf.get("in")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("out")+"/"+run));
		job.waitForCompletion(true);
	}
	
	private static int[] readTresholds(){
		int [] tresholds = new int[2];
		//Scanner reader = new Scanner(System.in);
		//System.out.println("Enter a number as the support treshold: ");
		//tresholds[0] = reader.nextInt();
		// Placeholder for testing
		tresholds[0] = 500;
		
		//System.out.println("Enter a number as the confidence treshold: ");
		//tresholds[1] = reader.nextInt();
		// Placeholder for testing
		tresholds[1] = 50;
		//reader.close();
		
		return tresholds;
	}
	

}

