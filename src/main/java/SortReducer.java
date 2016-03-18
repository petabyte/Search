import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by U0155811 on 3/15/2016.
 */
public class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text > {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for(Text value :values){
            context.write(key, value);
        }
    }

}
