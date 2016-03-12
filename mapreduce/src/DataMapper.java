

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DataMapper extends Mapper<Object, BytesWritable, BytesWritable, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	
	@Override
	public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
		
		byte[] data = value.getBytes();
		byte[] prevVal, newVal, nextBytes;
		int index;
		
		byte [] firstBytes = Arrays.copyOfRange(data, 0, 16);
		context.write(new BytesWritable(firstBytes), one);
		index = 16;
			
		// Need to do sliding window here, n 64-bit words result in n-1 128-bit values
		prevVal = firstBytes;
		while (index < data.length - 1) {
			nextBytes = Arrays.copyOfRange(data, index, index + 8);
			index += 8;
				
			newVal = new byte[16];
			System.arraycopy(prevVal, 8, newVal, 0, 8);
			System.arraycopy(nextBytes, 0, newVal, 8, 8);
				
			context.write(new BytesWritable(newVal), one);
			prevVal = newVal;
		}
	}

}
