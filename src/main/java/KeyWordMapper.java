import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by U0155811 on 3/17/2016.
 */
public class KeyWordMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text keyWordText = new Text();
    IntWritable countWritable = new IntWritable();
    DateFormat df = new SimpleDateFormat("yyyyMMdd");
    Calendar calendar = Calendar.getInstance();
    private KeywordParser parser = new KeywordParser();
    enum KeyWordError {
        NO_KEYWORD_FOUND
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] fileNameSplit = filename.split("_");
            String[] fileNameSplit2 = fileNameSplit[2].split("\\.");
            String fileDateString = fileNameSplit2[0];
            Date fileDate = df.parse(fileDateString);
            calendar.setTime(fileDate);
            int month = calendar.get(Calendar.MONTH);
            if(Calendar.MARCH == month || Calendar.APRIL == month){
                parser.parseKeyWord(value);
                if (parser.foundKeyWord()) {
                    String keyWord = parser.getKeyWord();
                    keyWordText.clear();
                    keyWordText.set(keyWord);
                    countWritable.set(1);
                    context.write(keyWordText, countWritable);
                } else {
                    System.err.println("Ignoring possibly corrupt input: " + value);
                    context.getCounter(KeyWordError.NO_KEYWORD_FOUND).increment(1);
                }
            }
        }catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        }catch(ParseException pe){
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}
