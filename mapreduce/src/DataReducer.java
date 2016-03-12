

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable> {
	
	private Map<BytesWritable, IntWritable> reducedVals;
	
	public DataReducer() {
		super();
		reducedVals = new HashMap<BytesWritable, IntWritable>();
	}

	@Override
	public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		// Found more than one instance of this key, so save for output
		if (sum > 1) {
			reducedVals.put(key, new IntWritable(sum));
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<BytesWritable, IntWritable> entry : reducedVals.entrySet()) {
			context.write(entry.getKey(), entry.getValue());
		}
		
		super.cleanup(context);
	}
}
