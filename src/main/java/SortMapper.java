import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by U0155811 on 3/15/2016.
 */
public class SortMapper extends Mapper<Text, Text, IntWritable, Text> {

    IntWritable keyIntWritable = new IntWritable();
    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            keyIntWritable.set(Integer.parseInt(value.toString()));
            context.write(keyIntWritable, key);
        }catch(NumberFormatException nfe){
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}
