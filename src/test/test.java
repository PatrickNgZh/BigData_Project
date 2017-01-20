package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class test {

	public static void main(String[] args) {
		String [] input = {"A","B","C","D","E"};
		ArrayList<String> test= getSubSets(input,3);
		System.out.println(test.size());

		for (int i=0; i<test.size();i++)
		{
			System.out.println(test.get(i) + " ");
		}
	}



	private static ArrayList<String> getSubSets(String[] input, int k) {
		Set<String> subsets = new HashSet<String>();
		for (int j=input.length-1;j>=0; j--){
			System.out.println("test  "+j+"   ");
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
				System.out.println("test  "+String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
			else if(j==0){
				String [] insert =Arrays.copyOfRange(input, 1, input.length);
				subsets.add(String.join(",", insert));
				System.out.println("test  "+String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
			else if (j==input.length-1){
				String [] insert =	Arrays.copyOfRange(input, 0, input.length-1);
				subsets.add(String.join(",", insert));
				System.out.println("test  "+String.join(",", insert));
				if (insert.length >k){
					subsets.addAll(getSubSets(insert,k));
				}
			}
			
		}
		
		ArrayList<String> subset_list = new ArrayList<String>();
		subset_list.addAll(subsets);
		return subset_list;
	}
}
