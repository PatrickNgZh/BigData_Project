import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

public class associationRules {

	static double confidenceThreshold = FreqItems.getConfidenceTreshold();
	private static HashMap<String[], Integer> freqItemsetsHashMap = readInput(conf);
	
	//main method
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		for (i in all large frequent itemsets of k > 2) {
			ArrayList<Set<String>> consequentArray = new ArrayList<Set<String>>();
			for (int j = 0; j < i.length(); j++) {
				Set<String> currentItem = new HashSet<String>();
				currentItem.add(i[j]);
				consequentArray.add(j, currentItem);
			}
			genRules(frequentItemset, consequentArray);
		}
		
	}	
	public void genRules(Set<String> freqItemset, ArrayList<Set<String>> setOfConsequents) {

		int k = freqItemset.length;
		int m = setOfConsequents.get(0).size();
		
		if (k > m + 1) {
		
			ArrayList<Set<String>> superSetOfConsequents = aprioriGen(setOfConsequents);
			
			for (Set<String> consequent : superSetOfConsequents) {
				double confidence = 0.0;
				Set<String> antecedent = new HashSet<String>();
				antecedent.addAll(freqItemset);
				antecedent.removeAll(consequent);
				confidence = (double)getSupport(freqItemset) / getSupport(antecedent);
				
				if (confidence >= FreqItems.getConfidenceTreshold()) {
					//write the association rule to an output file
					outputRules(conf, itemSubsetSet[i], consequent, confidence);
				} else {
					superSetOfConsequents.remove(superSetOfConsequents.indexOf(consequent));
				}
				genRules(freqItemset, superSetOfConsequents);	
			}
		}
	}

//	
//	static void getSubsets(String[] freqItemset, boolean[] used, int startIndex, int currentSize, int k) {
//		if (currentSize == k) {
//			for (int i = 0; i < freqItemset.length; i++) {
//				
//			}
//		}
//		
//	}
//	
//	private static ArrayList<String> getSubSets(String[] input, int k) {
//		Set<String> subsets = new HashSet<String>();
//		for (int j=input.length-1;j>=0; j--){
//			if (j>0 && j<input.length-1){
//				String [] toCheck1 =Arrays.copyOfRange(input, 0, j);
//				String [] toCheck2 = Arrays.copyOfRange(input, j+1,input.length);
//				String [] insert = new String [toCheck1.length+toCheck2.length];
//
//				for (int i=0; i<insert.length;i++){
//					if (i <toCheck1.length){
//						insert [i]=toCheck1[i];
//					}else{
//						insert [i]=toCheck2[i-toCheck1.length];
//					}
//				}
//				subsets.add(String.join(",", insert));
//				if (insert.length >k){
//					subsets.addAll(getSubSets(insert,k));
//				}
//			}
//			else if(j==0){
//				String [] insert =Arrays.copyOfRange(input, 1, input.length);
//				subsets.add(String.join(",", insert));
//				if (insert.length >k){
//					subsets.addAll(getSubSets(insert,k));
//				}
//			}
//			
//			else if (j==input.length-1){
//				String [] insert =	Arrays.copyOfRange(input, 0, input.length-1);
//				subsets.add(String.join(",", insert));
//				if (insert.length >k){
//					subsets.addAll(getSubSets(insert,k));
//				}
//			}
//		}
//		ArrayList<String> subset_list = new ArrayList<String>();
//		subset_list.addAll(subsets);
//		return subset_list;
//	}
	
	// method to generate all possible sets of k+1 lengths from an array of sets of length k 
	private static ArrayList<Set<String>> aprioriGen(ArrayList<Set<String>> consequentsArray) {
		
		ArrayList<Set<String>> newConsequentArray = new ArrayList<Set<String>>();
		int k = consequentsArray.get(0).size();
		
		for (int i = 0; i < consequentsArray.size() - 1; i++) {
			
			ArrayList<String> temp1 = new ArrayList<String>();
			temp1.addAll(consequentsArray.get(i));
			
			for (int j = i + 1; j < consequentsArray.size(); j++) {
				
				ArrayList<String> temp2 = new ArrayList<String>();
				temp2.addAll(consequentsArray.get(j));
				Set<String> setToAdd = new HashSet<String>();
				
				if (k == 1) {
					
					setToAdd.add(temp1.get(j) + temp2.get(j));
					newConsequentArray.add(setToAdd);
					
				} else {
					
					boolean flag = true;
					for (int t = 0; t < k; t++) {
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
	
	
	//method that subtracts a subset from a set and returns the result
	public static String[] getSubset(String[] set, String[] subset) {
		ArrayList<String> result = new ArrayList<String>();
		for (String tmp : set) {
			if (Arrays.asList(subset).contains(tmp)) {
				result.add(tmp);
			}
		}
		return listToStringArray(result);
	}
	
	//method of converting an arrayList to a String array
	public static String[] listToStringArray(ArrayList<String> inputArray) {
		String[] result = new String[inputArray.size()];
		for (int i = 0; i < inputArray.size(); i++) {
			result[i] = inputArray.get(i);
		}
		return result;
	}

	//method to compare confidence with threshold
	public static boolean isConfident(float conf) {
		return conf >= FreqItems.getConfidenceTreshold();
	}
	
	//method to get support from hashmap of frequent itemsets
	public static double getSupport(Set<String> hashKey) {
		return freqItemsetsHashMap.get(hashKey);
	}
	
	//method to calculate interest
	public static double getInterest() {
		double interest = 0.0;
		return interest;
	}

    private HashMap<Set<String>, Integer> readInput(Configuration conf) throws IOException {
    	HashMap<Set<String>, Integer> freqItemsetHashMap = new HashMap<Set<String>, Integer>();
    	FileSystem fs = FileSystem.get(conf);
    	String run = String.valueOf(conf.getInt("run",0)-1); 
    	String filename = "/"+run+"/"+run+"-r-00000";
    	Path pathInput=new Path(conf.get("tmp") +  filename);
    	BufferedReader read = new BufferedReader(new InputStreamReader(fs.open(pathInput)));

    	String line;
    	while( (line = read.readLine()) != null) {
    		String [] keyValue = line.split("\\t");
    		Set<String> keySet = new HashSet<String>(Arrays.asList(keyValue[0].split(",")));
    		freqItemsetHashMap.put(keySet, Integer.parseInt(keyValue[1]));		
    	}
    	read.close();		
    	return freqItemsetHashMap;
    }
	
    // method to write rules to a file
    private void outputRules(Configuration conf, String[] freqItemsubset, String[] consequent, double confidence) {
    	String filename = "/out/" + "output_rules";
		Path pathOutput = new Path(filename);
		FileSystem fs = FileSystem.get(conf);
		Writer write = new BufferedWriter(new OutputStreamWriter(fs.create(pathOutput)));
		
		write.write("{");
		for (int i = 0; i < freqItemsubset.length; i++ ) {
			write.write(freqItemsubset[i] + ",");
		}
		write.write("} -> ");
		for (int i = 0; i < consequent.length; i++ ) {
			if (i < consequent.length - 1) {
				write.write(consequent[i] + ",");
			} else {
				write.write(consequent[i]);
			}
		}
		write.write((int)confidence + "\n");
    }
}


