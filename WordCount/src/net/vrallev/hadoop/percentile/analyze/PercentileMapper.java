package net.vrallev.hadoop.percentile.analyze;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author Ralf Wondratschek
 */
public class PercentileMapper implements Mapper<LongWritable, Text, IntWritable, Text> {

    private int[] mLinesTotal;
    private int[][] mLinesDirection;

    @Override
    public void configure(JobConf job) {
        int countTotal = job.getInt("count_total", -1);
        int[] countDirection = new int[8];
        for (int i = 0; i < countDirection.length; i++) {
            countDirection[i] = job.getInt("count_" + (i * 45), -1);
        }

        mLinesTotal = new int[100];
        mLinesDirection = new int[8][100];

        fillPlaceholder(countTotal, mLinesTotal);
        for (int i = 0; i < mLinesDirection.length; i++) {
            fillPlaceholder(countDirection[i], mLinesDirection[i]);
        }
    }

    @Override
    public void close() throws IOException {
    }

    private IntWritable mKey = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        String valString = value.toString();
        String[] val = valString.split(";");
        String direction = val[0].substring(val[0].indexOf("_") + 1);

        val = val[1].split("_");
        int lineNumberDirection = Integer.parseInt(val[0]);
        int lineNumberTotal = Integer.parseInt(val[1]);

        int lineCountForNumber = getLineCountForNumber(lineNumberTotal, mLinesTotal);
        mKey.set(lineNumberTotal);
        while (lineCountForNumber > 0) {
            lineCountForNumber--;
            output.collect(mKey, new Text(valString + "t"));
        }

        lineCountForNumber = getLineCountForNumber(lineNumberDirection, mLinesDirection[Integer.parseInt(direction) / 45]);
        mKey.set(lineNumberDirection);
        while (lineCountForNumber > 0) {
            lineCountForNumber--;
            output.collect(mKey, value);
        }
    }

    private static void fillPlaceholder(int simulationCount, int[] placeholder) {
        double stepSize = simulationCount / (double) placeholder.length;
        double line = 0;

        if (simulationCount <= placeholder.length) {
            for (int i = 0; i < placeholder.length; i++) {
                placeholder[i] = 1 + (int) line;
                line += stepSize;
            }
        } else {
            for (int i = 0; i < placeholder.length; i++) {
                line += stepSize;
                placeholder[i] = (int) Math.round(line);
            }
        }
    }

    private static int getLineCountForNumber(int lineNumber, int[] placeHolder) {
        int res = 0;
        for (int value : placeHolder) {
            if (value == lineNumber) {
                res++;
            } else if (lineNumber < value) {
                return res;
            }
        }
        return res;
    }
}
