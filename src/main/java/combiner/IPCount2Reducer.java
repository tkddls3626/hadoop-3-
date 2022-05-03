package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IPCount2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int ipCount = 0;
        for (IntWritable value : values) {
            ipCount += value.get();

        }
        context.write(key, new IntWritable(ipCount));

    }
}