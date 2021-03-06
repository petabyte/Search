import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by U0155811 on 3/15/2016.
 */
public class SortDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: SortDriver <input dir> <output dir>\n");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(SortDriver.class);
        job.setJobName("SortDriver");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setSortComparatorClass(IntComparator.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /*
     * The main method calls the ToolRunner.run method, which
     * calls an options parser that interprets Hadoop command-line
     * options and puts them into a Configuration object.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SortDriver(), args);
        System.exit(exitCode);
    }
}
