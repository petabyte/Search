import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Created by U0155811 on 3/17/2016.
 */
public class TopMapperReduceTest {
    /*
    * Declare harnesses that let you test a mapper, a reducer, and
    * a mapper and a reducer working together.
    */
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        TopMapper mapper = new TopMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        TopReducer reducer = new TopReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);

    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mappers.
     *
     */
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("1\tpartnership\tpartnership"))
                .withInput(new LongWritable(1), new Text("44\t\"related party\"\t\"related party\""))
                .withInput(new LongWritable(1), new Text("44\t\"related party\"\t\"related these party\""))
                .withInput(new LongWritable(1), new Text("103809\tlimitations charitable contribution deduction\tcharitable contribution"))
                .withInput(new LongWritable(1), new Text("103809\tlimitations charitable contribution deduction\tcharitable contribution"));
        mapDriver.withOutput(new Text("\"related party\""), new IntWritable(1));
        mapDriver.withOutput(new Text("\"related these party\""), new IntWritable(1));
        mapDriver.withOutput(new Text("charitable contribution"), new IntWritable(1));
        mapDriver.withOutput(new Text("charitable contribution"), new IntWritable(1));
        mapDriver.runTest();
        assertTrue(mapDriver.getCounters().findCounter(TopMapper.Concept.DOUBLE_QUOTED_CONCEPT).getValue() == 2);
    }

    /*
  * Test the reducer.
  */
    @Test
    public void testReducer() throws IOException {

        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("charitable contribution"), values);
        reduceDriver.withOutput(new Text("charitable contribution"), new IntWritable(4));
        reduceDriver.runTest();
    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withInput(new LongWritable(1), new Text("1\tpartnership\tpartnership"))
                .withInput(new LongWritable(1), new Text("44\t\"related party\"\t\"related party\""))
                .withInput(new LongWritable(1), new Text("103809\tlimitations charitable contribution deduction\tcharitable contribution"))
                .withInput(new LongWritable(1), new Text("103809\tlimitations charitable contribution deduction\tcharitable contribution"));
        mapReduceDriver.withOutput(new Text("\"related party\""), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("charitable contribution"), new IntWritable(2));
        mapReduceDriver.runTest();

    }

}
