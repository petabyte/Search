import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by U0155811 on 3/15/2016.
 */
public class TopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    static enum Concept{
        DOUBLE_QUOTED_CONCEPT,
        MULTI_WORD_CONCEPT
    }
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
            if(conceptRecordLowerCase.matches("^\"\\w+.+\"$")){
                context.getCounter(Concept.DOUBLE_QUOTED_CONCEPT).increment(1);
            }
            String[] conceptSplit = conceptRecordLowerCase.split("\\s+");
            if(conceptSplit.length > 1) {
                context.getCounter(Concept.MULTI_WORD_CONCEPT).increment(1);
                conceptText.set(conceptRecordLowerCase);
                context.write(conceptText, concepTextCount);
            }

        } catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}
