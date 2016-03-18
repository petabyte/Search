import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by U0155811 on 3/17/2016.
 */
public class KeyWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable keyWordsCountIntWritable = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                      Context context)
            throws IOException, InterruptedException {

        int keyWordsCount = 0;
        for (IntWritable value : values) {
            keyWordsCount += value.get();
        }
        keyWordsCountIntWritable.set(keyWordsCount);
        context.write(key, keyWordsCountIntWritable);
    }
}