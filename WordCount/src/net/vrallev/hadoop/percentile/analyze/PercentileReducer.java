package net.vrallev.hadoop.percentile.analyze;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Ralf Wondratschek
 */
public class PercentileReducer implements Reducer<IntWritable, Text, Text, Text> {

    private int mCountTotal;
    private int[] mCountDirection;

    @Override
    public void configure(JobConf job) {
        mCountTotal = job.getInt("count_total", -1);
        mCountDirection = new int[8];
        for (int i = 0; i < mCountDirection.length; i++) {
            mCountDirection[i] = job.getInt("count_" + (i * 45), -1);
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        while (values.hasNext()) {

            Text valText = values.next();
            String valString = valText.toString();
            String[] val = valString.split(";");
            String direction = val[0].substring(val[0].indexOf("_") + 1);

            val = val[1].split("_");

            double percentile;
            if (valString.endsWith("t")) {
                int lineNumberTotal = Integer.parseInt(val[1].substring(0, val[1].length() - 1));
                percentile = lineNumberTotal / (double) mCountTotal;
            } else {
                int lineNumberDirection = Integer.parseInt(val[0]);
                percentile = lineNumberDirection / (double) mCountDirection[Integer.parseInt(direction) / 45];
            }

            int label = (int) (percentile * 100);
            StringBuilder builder = new StringBuilder("Q_");
            if (label == 100) {
                builder.append("1.00");
            } else if (label < 10) {
                builder.append("0.0").append(label);
            } else {
                builder.append("0.").append(label);
            }

            output.collect(new Text(builder.toString()), valText);
        }
    }
}
