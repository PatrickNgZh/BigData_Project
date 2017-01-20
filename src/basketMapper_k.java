import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class basketMapper_k extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text item = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value , Context context) throws IOException, InterruptedException {
		String text = value.toString();
		String [] input = text.split(",");
		
		
		Arrays.sort(input); //consider doing that once before the input is passed to any mapper
		
		ArrayList<String> candidates = getCandidates(context);
		ArrayList<String> subsets = getSubSets(input,context.getConfiguration().getInt("run", 0));
		ArrayList<String> result = new ArrayList<String>();
		
		if (subsets.size()>candidates.size()){
			for (int i=0;i<candidates.size();i++){
				if (subsets.contains(candidates.get(i))){
					result.add(candidates.get(i));
				}
			}
		}else{
			for (int i=0;i<subsets.size();i++){
				if (candidates.contains(subsets.get(i))){
					result.add(subsets.get(i));
				}
			}
		}
		
		for (int i=0;i<result.size();i++){
			item.set(result.get(i));
			context.write(item, one);
		}		
		
		/* TBD
		for (int i=0;i<input.length;i++){
			item.set(input[i]);
			context.write(item, one);			
		}*/
	}

	private static ArrayList<String> getSubSets(String[] input, int k) {
		Set<String> subsets = new HashSet<String>();
		for (int j=input.length-1;j>=0; j--){
			if (j>0 && j<input.length-1){
				String [] toCheck1 =Arrays.copyOfRange(input, 0, j);
				String [] toCheck2 = Arrays.copyOfRange(input, j+1,input.length);
				String [] insert = new String [toCheck1.length+toCheck2.length];

				for (int i=0; i<insert.length;i++){
					if (i <toCheck1.length){
						insert [i]=toCheck1[i];
					}else{
						insert [i]=toCheck2[i-toCheck1.length];
					}
				}
				subsets.add(String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
			else if(j==0){
				String [] insert =Arrays.copyOfRange(input, 1, input.length);
				subsets.add(String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
			else if (j==input.length-1){
				String [] insert =	Arrays.copyOfRange(input, 0, input.length-1);
				subsets.add(String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
		}
		
		ArrayList<String> subset_list = new ArrayList<String>();
		subset_list.addAll(subsets);
		return subset_list;
	}
	/*
	private ArrayList<String> getSubSets(String[] input) {
		ArrayList<String> subsets = new ArrayList<String>();
		if (input == null){
			return null;
		}
		else{
			for (int i=0,j=input.length-1;i<input.length;i++, j--){
				String toCheck1 = String.join(",",Arrays.copyOfRange(input, 0, j));
				String toCheck2 = String.join(",",Arrays.copyOfRange(input, j+1,input.length-1));
				if(toCheck2 == null ||toCheck2 != "" && toCheck1 == null ||toCheck1 != "") {
					return null;
				}else if (toCheck1 == null ||toCheck1 == "" && toCheck2 != null ||toCheck2 != ""){
					subsets.set(i,toCheck2);
					ArrayList<String> insert = getSubSets(toCheck2.split(","));
					if (insert!= null){
						subsets.addAll(insert);
					}
				}
				else if(toCheck2 == null ||toCheck2 == "" && toCheck1 != null ||toCheck1 != ""){
					subsets.set(i,toCheck1);
					ArrayList<String> insert = getSubSets(toCheck1.split(","));
					if (insert!= null){
						subsets.addAll(insert);
					}
				}
				else {
					String combined = toCheck1+","+toCheck2;
					subsets.set(i,combined);
					ArrayList<String> insert = getSubSets(combined.split(","));
					if (insert!= null){
						subsets.addAll(insert);
					}
				} 
			}
			return subsets;
		}
	}*/

	private ArrayList<String>  getCandidates(Context context) throws IOException {
		ArrayList<String> candidates = new ArrayList<String>();
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		String run = String.valueOf(context.getConfiguration().getInt("run",0)-1); 
		String filename = "/"+run+"/"+run+"-r-00000";
		Path pathInput=new Path(context.getConfiguration().get("tmp") +  filename);
		BufferedReader read=new BufferedReader(new InputStreamReader(fs.open(pathInput)));
		
		String line;
		while( (line = read.readLine()) != null) {
		    String [] keyValue = line.split("\\t");
	    	candidates.add(keyValue[0]);		
		}
		
		read.close();		
		
		return candidates;
	}

}
