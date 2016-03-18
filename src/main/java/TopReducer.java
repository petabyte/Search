import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by U0155811 on 3/15/2016.
 */
public class TopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable conceptCombinationCounter = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int conceptCombinationCount = 0;
        for (IntWritable value : values) {
            conceptCombinationCount += value.get();
        }
        conceptCombinationCounter.set(conceptCombinationCount);
        context.write(key, conceptCombinationCounter);
    }
}
