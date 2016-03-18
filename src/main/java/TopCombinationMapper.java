import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by U0155811 on 3/17/2016.
 */
public class TopCombinationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text conceptText = new Text();
    IntWritable concepTextCount = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        //Split the records tab delimited
        String[] recordSplit = line.split("\\t+");
        try {
            String keywordRecord = recordSplit[1];
            String conceptRecord = recordSplit[2];
            String conceptRecordLowerCase = conceptRecord.toLowerCase();
            conceptText.set(conceptRecordLowerCase);
            context.write(conceptText, concepTextCount);
        } catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}