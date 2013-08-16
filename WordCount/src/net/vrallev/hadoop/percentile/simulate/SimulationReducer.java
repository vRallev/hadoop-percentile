package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

public class SimulationReducer implements Reducer<DoubleWritable, Text, Text, Text> {

    private int mNumbersAfterComma;
    private int mNumberOfSimulations;

    private int mLine;
    private int[] mDirectionLine;

    @Override
	public void configure(JobConf conf) {
        mNumberOfSimulations = Integer.parseInt(conf.get(SimulationTool.NUMBER_OF_SIMULATIONS));
        mNumbersAfterComma = Integer.parseInt(conf.get(SimulationTool.NUMBERS_AFTER_COMMA));

        mLine = 1;
        mDirectionLine = new int[8];
        for (int i = 0; i < mDirectionLine.length; i++) {
            mDirectionLine[i] = 1;
        }
    }

	@Override
	public void close() throws IOException {
		
	}

    private Text mKeyText = new Text();
    private Text mValueText = new Text();

	@Override
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        if (key.get() == SimulationMapper.COUNT_KEY.get()) {
            // special key for counting the number of simulations
            int[] count = new int[8];
            while (values.hasNext()) {
                int index = Integer.parseInt(values.next().toString()) / 45;
                count[index]++;
            }

            int total = 0;
            for (int i = 0; i < count.length; i++) {
                count[i] *= mNumberOfSimulations;
                // Use "total_" as prefix, so we can create a different file name in the OutFormat class.
                output.collect(new Text("total_" + (i * 45)), new Text(String.valueOf(count[i])));
                total += count[i];
            }

            // The if statement is true the first time reduce is called, because COUNT_KEY is the smallest possible double value,
            // so we know, that the output begins.
            System.out.println("\n\nOutput IO begins\n\n");

            output.collect(new Text("total_count"), new Text(String.valueOf(total)));
            return;
        }

        keyToText(key, mNumbersAfterComma);

        while (values.hasNext()) {
            // sometimes same simulation result (e.g. if numbers after comma is very small)
            String valString = values.next().toString();
            int directionIndex = Integer.parseInt(valString.split("_")[1]) / 45;

            // append line number for direction and total
            StringBuilder valBuilder = new StringBuilder(valString);
            valBuilder.append(';').append(mDirectionLine[directionIndex]).append('_').append(mLine);

            mValueText.set(valBuilder.toString());
            output.collect(mKeyText, mValueText);

            // increase line numbers
            mLine++;
            mDirectionLine[directionIndex]++;
        }
    }

    /**
     *
     * @param key The simulation value.
     * @param numbersAfterComma How many numbers after the comma are displayed.
     */
    private void keyToText(DoubleWritable key, int numbersAfterComma) {
        double val = key.get();
        StringBuilder builder = new StringBuilder(String.valueOf(val));
        if (val >= 0) {
            // add gap, negative values start with minus
            builder.insert(0, " ");
        }

        while(builder.length() - builder.indexOf(".") - 1 < numbersAfterComma) {
            builder.append('0');
        }

        mKeyText.set(builder.toString());
    }
}
