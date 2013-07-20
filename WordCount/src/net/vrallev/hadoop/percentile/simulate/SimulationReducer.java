package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class SimulationReducer implements Reducer<DoubleWritable, Text, Text, Text> {

    private int mNumbersAfterComma;
    private int mNumberOfSimulations;

    @Override
	public void configure(JobConf conf) {
        mNumberOfSimulations = Integer.parseInt(conf.get(SimulationTool.NUMBER_OF_SIMULATIONS, String.valueOf(SimulationMapper.DEFAULT_NUMBER_OF_SIMULATIONS)));
        mNumbersAfterComma = Integer.parseInt(conf.get(SimulationTool.NUMBERS_AFTER_COMMA, String.valueOf(SimulationMapper.DEFAULT_NUMBERS_AFTER_COMMA)));
    }

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        if (key.get() == SimulationMapper.COUNT_KEY.get()) {
            int count = 0;
            for (; values.hasNext(); count++) {
                values.next();
            }
            count *= mNumberOfSimulations;
            output.collect(SimulationMapper.COUNT_VALUE, new Text(String.valueOf(count)));
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append('(');

        while(values.hasNext()) {
            builder.append('\'').append(values.next()).append('\'');
            if (values.hasNext()) {
                builder.append(',');
            }
        }

        builder.append(");");

        output.collect(keyToText(key, mNumbersAfterComma), new Text(builder.toString()));
    }

    private static Text keyToText(DoubleWritable key, int numbersAfterComma) {
        StringBuilder builder = new StringBuilder(String.valueOf(key.get()));
        while(builder.length() - builder.indexOf(".") - 1 < numbersAfterComma) {
            builder.append('0');
        }
        return new Text(builder.toString());
    }
}
