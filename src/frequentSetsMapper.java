import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class frequentSetsMapper extends Mapper<Object, Text, Text, Text> {

	static double confidenceThreshold;
	private static HashMap<ArrayList<String>, Integer> freqItemsetsHashMap;
	private static ArrayList<String> outputList;
	static Configuration conf;
	private Text rule;
	private static Text measures;	

	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		confidenceThreshold = conf.getDouble("confidenceTreshold", 0);
		freqItemsetsHashMap = readInput(conf);
	}

	public void map(Object key, Text value , Context context) throws IOException, InterruptedException {
		outputList = new ArrayList<String>();
		measures = new Text();
		rule = new Text();
		
		String text = value.toString();
		String [] input = text.split("\\t")[0].split(",");
		Arrays.sort(input);

		ArrayList<String> currentSet = new ArrayList<String>(Arrays.asList(input));

		ArrayList<ArrayList<String>> consequentArray = new ArrayList<ArrayList<String>>();
		ArrayList<String> tempForSet = new ArrayList<String>();
		tempForSet.addAll(currentSet);
		for (int innerLoop = 0; innerLoop < tempForSet.size(); innerLoop++) {
			ArrayList<String> currentItem = new ArrayList<String>();
			currentItem.add(tempForSet.get(innerLoop));
			consequentArray.add(currentItem);
		}
		genRules(currentSet, consequentArray);
		
		for (String temp : outputList) {
			String[] line =temp.split("\t\t");
			rule.set(line[0]);
			measures.set(line[1]);
			context.write(rule, measures);
		}
		
	}
	
	public static void genRules(ArrayList<String> freqItemset, ArrayList<ArrayList<String>> setOfConsequents) throws IOException {

		int k = freqItemset.size();
		int m = setOfConsequents.get(0).size();

		if (k > m) {

			ArrayList<ArrayList<String>> copy = new ArrayList<ArrayList<String>>();
			copy.addAll(setOfConsequents);

			for (ArrayList<String> consequent : setOfConsequents) {
				double confidence = 0.0;
				double interest = 0.0;
				ArrayList<String> antecedent = new ArrayList<String>();
				
				antecedent.addAll(freqItemset);
				antecedent.removeAll(consequent);
				
				confidence = (double)getSupport(freqItemset) / getSupport(antecedent);
				interest = confidence - ( (double)getSupport(consequent) / conf.getInt("basketsNumber", 1) );
				
				if (confidence >= confidenceThreshold) {
					
					outputRules(antecedent, consequent, confidence, interest);
					
				} else {
					
					copy.remove(copy.indexOf(consequent));
					
				}
			}
			
			if (copy.size() > 0) {
				
				ArrayList<ArrayList<String>> superSetOfConsequents = aprioriGen(copy);
				
				if (superSetOfConsequents.size() > 0) {
					
					genRules(freqItemset, superSetOfConsequents);
					
				} else {
					
					//System.out.println("No Supersets");
				}
			}
		}
	}
	
	// method to generate all possible sets of k+1 lengths from an array of sets of length k 
	private static ArrayList<ArrayList<String>> aprioriGen(ArrayList<ArrayList<String>> consequentsArray) {
		ArrayList<ArrayList<String>> newConsequentArray = new ArrayList<ArrayList<String>>();
		int k = consequentsArray.get(0).size();

		for (int i = 0; i < consequentsArray.size() - 1; i++) {

			ArrayList<String> temp1 = new ArrayList<String>();
			temp1.addAll(consequentsArray.get(i));

			for (int j = i + 1; j < consequentsArray.size(); j++) {

				ArrayList<String> temp2 = new ArrayList<String>();
				temp2.addAll(consequentsArray.get(j));
				ArrayList<String> setToAdd = new ArrayList<String>();

				if (k == 1) {

					setToAdd.add(temp1.get(0));
					setToAdd.add(temp2.get(0));
					newConsequentArray.add(setToAdd);

				} else {

					boolean flag = true;
					for (int t = 0; t < k - 1; t++) {
						if (temp1.get(t) != temp2.get(t)) {
							flag = false;
						}
					}

					if (flag == true) {

						for (int t = 0; t < k - 1; t++) {
							setToAdd.add(temp1.get(t));
						}

						setToAdd.add(temp2.get(k-1));
						newConsequentArray.add(setToAdd);
					}
				}
			}
		}
		return newConsequentArray; 
	}
	public static double getSupport(ArrayList<String> hashKey) {
		return freqItemsetsHashMap.get(hashKey);
	}
	
	private static void outputRules(ArrayList<String> antecedent, ArrayList<String> consequent, double confidence, double interest) throws IOException {

		String TBW = new String();
		TBW = "{";

		for (int i = 0; i < antecedent.size(); i++ ) {
			if (i < antecedent.size() - 1) {
				TBW += antecedent.get(i) + ",";
			} else {
				TBW += antecedent.get(i);
			}
		}
		TBW += "} -> ";

		for (int i = 0; i < consequent.size(); i++ ) {
			if (i < consequent.size() - 1) {
				TBW += consequent.get(i) + ",";
			} else {
				TBW += consequent.get(i);
			}
		}
		TBW += "\t\t" + confidence + "\t" + interest;
		outputList.add(TBW);
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
}
