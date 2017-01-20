import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class basketMapper_1 extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text item = new Text();
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value , Context context) throws IOException, InterruptedException {
		String text = value.toString();
		String [] input = text.split(",");
		
		for (int i=0;i<input.length;i++){
			item.set(input[i]);
			context.write(item, one);			
		}
	}

}
