package noMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class basketMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text item = new Text();
	private final static IntWritable one = new IntWritable(1);
	private ArrayList<String> candidates;
	private Configuration conf;

	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		if (conf.getInt("run", 0) !=1){
			candidates = getCandidates(conf);
		}
	}

	public void map(Object key, Text value , Context context) throws IOException, InterruptedException {

		String text = value.toString();
		String [] input = text.split(",");
		Arrays.sort(input);

		if (conf.getInt("run", 0) ==1){
			FirstMapper(input,context);
		}
		else{
			otherMapper(input,context);
		}
	}

	private void otherMapper(String[] input, Context context) throws IOException, InterruptedException {

		if (input.length>conf.getInt("run", 0)){


			ArrayList<String> result = new ArrayList<String>();

			if (candidates.size()>750 && input.length < 12){
				ArrayList<String> subsets = getSubSets(input,conf.getInt("run", 0));
				for (int i=0;i<subsets.size();i++){
					if (candidates.contains(subsets.get(i))){
						result.add(subsets.get(i));
					}
				}
			}
			else{
				for (int i=0;i<candidates.size();i++){
					String [] candidate = candidates.get(i).split(",");
					boolean match = true;
					
					for (int j=0;j<candidate.length;j++){
						if (!Arrays.asList(input).contains(candidate[j]))
						{
							match = false;
						}
					}
					
					if (match == true){
						result.add(candidates.get(i));
					}
				}
			}




			for (int i=0;i<result.size();i++){
				item.set(result.get(i));
				context.write(item, one);
			}		
		}
		else if (input.length==conf.getInt("run", 0)){
			String input_string = String.join(",",input);

			if (candidates.contains(input_string)){
				item.set(input_string);
				context.write(item, one);
			}
		}

	}

	private void FirstMapper(String[] input, Context context) throws IOException, InterruptedException {
		for (int i=0;i<input.length;i++){
			item.set(input[i]);
			context.write(item, one);			
		}

	}

	private static void getSubSets_void(List<String> superSet, int k, int id, Set<String> current,List<Set<String>> solution) {

		if (current.size() == k) {
			solution.add(new HashSet<>(current));
			return;
		}

		if (id == superSet.size()) return;
		String x = superSet.get(id);
		
		current.add(x);
		getSubSets_void(superSet, k, id+1, current, solution);
		
		current.remove(x);
		getSubSets_void(superSet, k, id+1, current, solution);
	}

	public static ArrayList<String> getSubSets(String[] input, int k) {
		List<Set<String>> set = new ArrayList<>();
		ArrayList <String> result = new ArrayList <>();

		ArrayList<String> input_list = new ArrayList<String>();
		for (int i=0;i<input.length;i++){
			input_list.add(input[i]);
		}

		getSubSets_void(input_list, k, 0, new HashSet<String>(), set);

		for (int i=0;i<set.size();i++){

			ArrayList<String> temp = new ArrayList<String>();
			temp.addAll(set.get(i));
			Collections.sort(temp);


			result.add(String.join(",",temp));
		}
		Collections.sort(result);
		return result;
	}


	private ArrayList<String>  getCandidates(Configuration conf) throws IOException {
		ArrayList<String> candidates = new ArrayList<String>();

		FileSystem fs = FileSystem.get(conf);
		String run = String.valueOf(conf.getInt("run",0)-1); 
		String filename = "/"+run+"/"+run+"-r-00000";
		Path pathInput=new Path(conf.get("tmp") +  filename);
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
