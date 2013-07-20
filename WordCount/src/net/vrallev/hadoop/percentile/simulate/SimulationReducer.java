package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class SimulationReducer implements Reducer<DoubleWritable, Text, Text, Text> {

	@Override
	public void configure(JobConf job) {

    }

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder builder = new StringBuilder();
        builder.append('(');

        while(values.hasNext()) {
            builder.append('\'').append(values.next()).append('\'');
            if (values.hasNext()) {
                builder.append(',');
            }
        }

        builder.append(");");

        output.collect(new Text(String.valueOf(key.get())), new Text(builder.toString()));
    }
}
