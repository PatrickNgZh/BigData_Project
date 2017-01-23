package withMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
@SuppressWarnings("unused")

public class FreqItems {
	public static Configuration conf;

	private static int counter;

	public static void main(String[] args) throws Exception {
		conf = new Configuration();

		counter = 0;

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("./out"), true);
		fs.delete(new Path("./tmp"), true);
		fs.delete(new Path("./enhanced_in"), true);
		fs.delete(new Path("./outputRules"), true);

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Please define parameter: <in>");
			System.exit(2);
		}
		conf.set("in", args[0]);
		conf.set("out", "out");
		conf.set("tmp", "tmp");
		conf.set("enhanced_in", "enhanced_in");


		double [] tresholds = readThresholds();	
		conf.setInt("supportTreshold", (int)tresholds[0]);
		conf.setDouble("confidenceTreshold", tresholds[1]);


		int run = 1;
		boolean MoreFrequents = true;
		while (MoreFrequents)
		{
			System.out.print("...");

			runJob_freq(run);
			MoreFrequents = generateCandidates(run);
			if (run ==1){
				onlyfrequent();
			}

			run+=1;
		}
		System.out.println("All frequent itemsets can be found in the Out folders");
		conf.setInt("basketsNumber", counter);

		/// Association rule building

		runJob_rules();
		
		System.out.println();
		System.out.println("All association rules can now be found in the 'out' folder");
		
		System.exit(0);
	}

	

//	private static void printrules() throws IOException {
//		String filename = "/output_rules";
//		Path pathOutput = new Path(conf.get("out") + filename);
//		FileSystem fs = FileSystem.get(conf);
//		Writer write = new BufferedWriter(new OutputStreamWriter(fs.create(pathOutput)));
//		for (int i =0; i<outputList.size();i++){
//			write.write(outputList.get(i));
//		}
//		write.close();
//	}

	private static void onlyfrequent() throws IOException {
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
			counter += 1;
			
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

	private static boolean generateCandidates(int run) throws Exception {
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
			System.out.println();
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
	private static void runJob_rules() throws ClassNotFoundException, IOException, InterruptedException {
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Run");
		job.setJarByClass(FreqItems.class);
		job.setMapperClass(frequentSetsMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);	
		int run = conf.getInt("run",0);
		for (int j = run-1; j >= 2; j--) {
			Path inputPath = new Path(conf.get("out")+"/"+String.valueOf(j));
			MultipleInputs.addInputPath(job, inputPath , TextInputFormat.class, frequentSetsMapper.class);
		}
		FileOutputFormat.setOutputPath(job, new Path("./outputRules"));
		job.waitForCompletion(true);
	}
	
	private static void runJob_freq (int run) throws Exception
	{	

		conf.setInt("run", run);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Run");
		job.setJarByClass(FreqItems.class);
		job.setMapperClass(basketMapper.class);

		job.setCombinerClass(itemCombiner.class);
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

	private static double[] readThresholds(){
		double [] thresholds = new double[2];
		Scanner reader = new Scanner(System.in);
		System.out.println("Enter a number as the support treshold: ");
		thresholds[0] = reader.nextDouble();
		// Placeholder for testing
		//thresholds[0] = 100;

		System.out.println("Enter a number as the confidence treshold: ");
		thresholds[1] = reader.nextDouble();
		// Placeholder for testing
		//thresholds[1] = 0.5;
		reader.close();

		return thresholds;
	}

	private static HashMap<ArrayList<String>, Integer> readInput(Configuration conf) throws IOException {
		HashMap<ArrayList<String>, Integer> freqItemsetHashMap = new HashMap<ArrayList<String>, Integer>();
		FileSystem fs = FileSystem.get(conf);
		int run = conf.getInt("run",0);

		for (int j = run-1; j > 0; j--) {
			String filename = "/"+j+"/"+j+"-r-00000";
			Path pathInput=new Path(conf.get("out") +  filename);
			BufferedReader read = new BufferedReader(new InputStreamReader(fs.open(pathInput)));

			String line;
			while( (line = read.readLine()) != null) {
				String [] keyValue = line.split("\\t");
				ArrayList<String> keySet = new ArrayList<String>(Arrays.asList(keyValue[0].split(",")));
				freqItemsetHashMap.put(keySet, Integer.parseInt(keyValue[1]));		
			}
			read.close();		
		}
		return freqItemsetHashMap;
	}

	private static ArrayList<ArrayList<String>> readInputForRules(Configuration conf) throws IOException {
		ArrayList<ArrayList<String>> freqItemsets = new ArrayList<ArrayList<String>>();
		FileSystem fs = FileSystem.get(conf);
		int run = conf.getInt("run",0);

		for (int j = run-1; j >= 2; j--) {
			String filename = "/"+j+"/"+j+"-r-00000";
			Path pathInput=new Path(conf.get("out") +  filename);
			BufferedReader read = new BufferedReader(new InputStreamReader(fs.open(pathInput)));

			String line;
			while( (line = read.readLine()) != null) {
				String [] keyValue = line.split("\\t")[0].split(",");
				Arrays.sort(keyValue);
				
				ArrayList<String> currentSet = new ArrayList<String>(Arrays.asList(keyValue));
				freqItemsets.add(currentSet);		
			}
			read.close();		
		}
		return freqItemsets;
	}

}

