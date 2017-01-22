import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class itemReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable sum = new IntWritable();
	private MultipleOutputs<Text, IntWritable> mos;
	
	@Override
    public void setup(Context context){
       mos = new MultipleOutputs<Text, IntWritable>(context);
    }
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String run = String.valueOf(context.getConfiguration().getInt("run", 0));
		int count = 0;
		
		
		for (IntWritable val : values) {
			count += val.get();
		}
		
		if (context.getConfiguration().getFloat("supportTreshold",0) <= count){
			sum.set(count);
			mos.write(run,key, sum);
		}
	}
	

	 public void cleanup(Context context) throws IOException, InterruptedException {
		 mos.close();
	 }

}
