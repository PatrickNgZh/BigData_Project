package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;

public class test {
	public static void main(String[] args){
		
		System.out.println(checkResults("butter,whole milk,yogurt"));

/*
		String[] result = new String[10];
		for (int i=0;i<10;i++){
			result[i]=String.valueOf(i);
		}
		ArrayList<String> res = getSubSets(result,2);

		Collections.sort(res);

		for (int i=0;i<res.size();i++){
			System.out.println(i+"\t"+res.get(i));
		}
*/
	}


	private static void getSubSets_void(List<String> superSet, int k, int id, Set<String> current,List<Set<String>> solution) {
		//enhance and adapt!!!!http://stackoverflow.com/questions/12548312/find-all-subsets-of-length-k-in-an-array

		//successful stop clause
		if (current.size() == k) {
			solution.add(new HashSet<>(current));
			return;
		}

		//unseccessful stop clause
		if (id == superSet.size()) return;
		String x = superSet.get(id);
		current.add(x);
		//"guess" x is in the subset
		getSubSets_void(superSet, k, id+1, current, solution);
		current.remove(x);
		//"guess" x is not in the subset
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
			System.out.println(String.join(",",temp));
			

			result.add(String.join(",",temp));
		}
		Collections.sort(result);
		//System.out.println(result);
		return result;
	}
	
	private static int checkResults(String string) {
		int count=0;
		try {
			BufferedReader bReader = new BufferedReader(new FileReader("./enhanced_in/data_for_project"));
			String line;
			while ((line = bReader.readLine()) != null) {
				String[] items = string.split(",");
				
				Boolean flag = true;
				for(int k=0; k<items.length; k++) {	
					if(!line.contains(items[k]))
						flag = false;
				}
				
				if(flag)
					count += 1;
			}
			bReader.close();
		}
		catch (Exception e) {
			System.out.println("Error reading input file.");
		}
		return count;
	}
}