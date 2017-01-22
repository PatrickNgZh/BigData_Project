package withMapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class itemCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable sum = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable val : values) {
			count += val.get();
		}
		sum.set(count);
		context.write(key, sum);
	}

}
