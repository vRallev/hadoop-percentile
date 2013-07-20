package net.vrallev.hadoop.percentile;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class PercentileReducer implements Reducer<Text, DoubleWritable, Text, Text> {

	/*
	 * TODO delete
	 * 
	 * the goal of the reducer is to create an outputline for each word and assign the information how often the word occurs in all files The first two args are dependend from the map-output and the
	 * last two args specify the reduce output e.g. jens|1 / jens|5 (if you have 5 map outputs in terms of jens|1) The method is called once per unique map output key. The iterable allows you to
	 * iterate over all the values that were emitted for the given key
	 */

	
	@Override
	public void configure(JobConf job) {
		
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder builder = new StringBuilder();
        builder.append('(');

        while(values.hasNext()) {
            builder.append('\'').append(values.next().get()).append('\'');
            if (values.hasNext()) {
                builder.append(',');
            }
        }

        builder.append(");");

        output.collect(key, new Text(builder.toString()));

//        int sum = 0;
//        while (values.hasNext()) {
//            sum += values.next().get();
//        }
//        output.collect(key, new DoubleWritable(sum));

    }

}
