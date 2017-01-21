import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

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
		fs.delete(new Path("./enhanced_in"), true);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Please define parameters: <in> <out> <tmp> <enhanced_in>");
			System.exit(2);
		}
		conf.set("in", args[0]);
		conf.set("out", args[1]);
		conf.set("tmp", args[2]);
		conf.set("enhanced_in", args[3]);


		int [] tresholds = readTresholds();	
		conf.setInt("supportTreshold", tresholds[0]);
		conf.setFloat("confidenceTreshold", tresholds[1]);


		int run = 1;
		boolean MoreFrequents = true;
		while (MoreFrequents)
		{
			runJob(conf,run);
			MoreFrequents = generateCandidates(conf, run);
			if (run ==1){
				onlyfrequent(conf);
			}
			run+=1;
		}
		System.out.println("All frequent itemsets can be found in the Out folders");

		System.exit(0);

	}

	private static void onlyfrequent(Configuration conf) throws IOException {
		String filename = "/"+1+"/"+1+"-r-00000";
		Path pathFrequent=new Path(conf.get("out") +  filename);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader read_frequent=new BufferedReader(new InputStreamReader(fs.open(pathFrequent)));

		String line;
		ArrayList<String> frequent = new ArrayList <String>();


		while( (line = read_frequent.readLine()) != null) {
			String [] items = line.split("\\t");
			frequent.add(items[0]);
		}

		String input = "/data_for_project.txt";
		Path pathInput=new Path(conf.get("in")+input);
		BufferedReader read_input=new BufferedReader(new InputStreamReader(fs.open(pathInput)));

		ArrayList<String> inputList = new ArrayList <String>();		
		while( (line = read_input.readLine()) != null) {
			inputList.add(line);
		}


		Path newInput=new Path("./enhanced_in" +"/data_for_project" );
		Writer write = new BufferedWriter(new OutputStreamWriter(fs.create(newInput)));

		for (int i=0;i<inputList.size();i++){
			String[] basket = inputList.get(i).split(",");
			Arrays.sort(basket);
			ArrayList<String> newBasket = new ArrayList<String>();
			boolean relevant =false;
			for (int j=0;j<basket.length;j++){
				if (frequent.contains(basket[j])){
					newBasket.add(basket[j]);
				}
			}
			for (int j=0;j<newBasket.size();j++){
				if (j==newBasket.size()-1){
					write.write(newBasket.get(j)+"\n");
				}
				else{
					write.write(newBasket.get(j)+",");
				}
			}

		}
		read_frequent.close();
		read_input.close();
		write.close();

	}

	private static boolean generateCandidates(Configuration conf, int run) throws Exception {
		boolean frequents_found = false;
		String filename = "/"+String.valueOf(run)+"/"+String.valueOf(run)+"-r-00000";
		Path pathInput=new Path(conf.get("out") +  filename);
		FileSystem fs = FileSystem.get(conf);
		ArrayList<String> inputList = new ArrayList <String>();
		
		try {
			BufferedReader read=new BufferedReader(new InputStreamReader(fs.open(pathInput)));		

			String line;
			while( (line = read.readLine()) != null) {
				String [] items = line.split("\\t");
				inputList.add(items[0]);
			}
			read.close();
		}catch (IOException Exception){
			System.out.println("There are no more Candidates");
			return frequents_found;
		}

		Path pathOutput=new Path(conf.get("tmp") + filename );
		Writer write = new BufferedWriter(new OutputStreamWriter(fs.create(pathOutput)));

		if (run == 1 && inputList.size()>0){
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
			System.out.println("2nd run");
			for (int i=0;i<inputList.size();i++){
				String[] first = inputList.get(i).split(",");
				for (int y=i+1;y<inputList.size();y++){
					boolean match = true;
					String[] second = inputList.get(y).split(",");
					String [] candidate = new String[first.length+1];
					for (int x = 0;x<first.length-1;x++){
						//System.out.print(second[x] + "   " + second.length +" + ");
						//System.out.println(first[x] + "    " + first.length);
						//System.out.print(second[x+1] + "   " + second.length +" + ");
						//System.out.println(first[x+1] + "    " + first.length);
						if (second[x].equals(first[x])){
							//System.out.println("match  - run " + run);
						}else {
							match =false;
							//System.out.println("no match  - run " + run);

						}
					}
					if (match==true){
						for (int a=0;a<candidate.length-1;a++){
							candidate[a]=first[a];
						}
						candidate[candidate.length-1]=second[second.length-1];
						//System.out.println("match executed");
						//System.out.println("tbc for frequents: "+String.join(",",candidate));
						if (allSubSetsFrequent(candidate,run, fs, inputList)){
							String candidateString = String.join(",",candidate);
							write.write(candidateString+"\n");
							frequents_found=true;
						}
					}
					if (candidate[0] != null){
						/*						System.out.println(String.join(",",candidate));
						if (allSubSetsFrequent(candidate,run, fs, inputList)){
							String candidateString = String.join(",",candidate);
							write.write(candidateString+"\n");
							frequents_found=true;
						}*/
					}
				}
			}
		}

		write.close();	
		return frequents_found;
	}

	private static boolean allSubSetsFrequent(String[] candidate, int run, FileSystem fs, ArrayList<String> inputList ) throws Exception {
		boolean allFrequent = true;

		boolean[] discard = new boolean[candidate.length];
		Arrays.fill(discard, false);

		String[] allSubSets = new String[candidate.length];

		for (int i=0;i<candidate.length;i++){
			discard[i]=true;
			String [] insert = new String [candidate.length-1];
			Boolean jump = false;

			for (int j=0; j<candidate.length;j++)
			{	
				if (discard[j]==false){
					if (jump==false){
						insert[j]=candidate[j];
					}
					else {
						insert[j-1]=candidate[j];
					}
				}
				else{
					jump=true;
				} 
			}
			discard[i]=false;
			//System.out.println(String.join(",",insert));
			Arrays.sort(insert);
			allSubSets[i]=String.join(",",insert);

		}
		//System.out.println(String.join(",",candidate));

		for (int i=0 ;i<allSubSets.length;i++){
			//System.out.println("allsubsets "+String.join(",",allSubSets[i]));
			if (!inputList.contains(allSubSets[i])){
				allFrequent=false;
				//System.out.println("boooo "+String.join(",",allSubSets[i]));
			}else{
				//System.out.println("wooohooo "+String.join(",",allSubSets[i]));
			}
		}
		return allFrequent;
	}


	private static void runJob (Configuration conf, int run) throws Exception
	{	

		conf.setInt("run", run);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Run");
		job.setJarByClass(FreqItems.class);
		if (run==1){
			System.out.println("Run 1");
			job.setMapperClass(basketMapper_1.class);
		}else {
			job.setMapperClass(basketMapper_k.class);
		}

		//job.setCombinerClass(itemCombiner.class);
		job.setReducerClass(itemReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, String.valueOf(run), TextOutputFormat.class,IntWritable.class, Text.class);
		if (run==1){
			FileInputFormat.addInputPath(job, new Path(conf.get("in")));
		}else {
			FileInputFormat.addInputPath(job, new Path(conf.get("enhanced_in")));
		}
		FileOutputFormat.setOutputPath(job, new Path(conf.get("out")+"/"+run));
		job.waitForCompletion(true);
	}

	private static int[] readTresholds(){
		int [] tresholds = new int[2];
		//Scanner reader = new Scanner(System.in);
		//System.out.println("Enter a number as the support treshold: ");
		//tresholds[0] = reader.nextInt();
		// Placeholder for testing
		tresholds[0] = 100;

		//System.out.println("Enter a number as the confidence treshold: ");
		//tresholds[1] = reader.nextInt();
		// Placeholder for testing
		tresholds[1] = 70;
		//reader.close();

		return tresholds;
	}


}

