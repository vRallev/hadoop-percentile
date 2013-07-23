package net.vrallev.hadoop.percentile.analyze;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Ralf Wondratschek
 */
public class PercentileTool extends Configured implements Tool {

    public static final String ANALYZE = "analyze";
    public static final String INPUT_FOLDER = "input_analyze";
    public static final String OUTPUT_FOLDER = "output_analyze";
    public static final String SIMULATION_COUNT = "simulationCount";
    public static final String CLEAR_OUTPUT_FOLDER = "clearOutputFolder_analyze";

    private Path mInputFolder;
    private Path mOutputFolder;

    private File mSimulationCountFile;
    private int mCountTotal;
    private int[] mCountDirection;

    private boolean mClearOutputFolder;

    public PercentileTool(Properties properties) {
        mInputFolder = new Path(properties.getProperty(INPUT_FOLDER));
        mOutputFolder = new Path(properties.getProperty(OUTPUT_FOLDER));

        mSimulationCountFile = new File(properties.getProperty(SIMULATION_COUNT));

        mClearOutputFolder = Boolean.parseBoolean(properties.getProperty(CLEAR_OUTPUT_FOLDER, "false"));
    }

    @Override
    public int run(String[] args) throws Exception {
        parseSimulationCount(mSimulationCountFile);

        if (mClearOutputFolder) {
            FileUtil.fullyDelete(new File(mOutputFolder.toString()));
        }

        getConf().setInt("count_total", mCountTotal);
        for (int i = 0; i < mCountDirection.length; i++) {
            getConf().setInt("count_" + (i * 45), mCountDirection[i]);
        }

        JobConf conf = new JobConf(getConf(), PercentileTool.class);
        conf.setJobName("Analyze");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(PercentileMapper.class);
        // conf.setCombinerClass(SimulationReducer.class);
        conf.setReducerClass(PercentileReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(OutFormat.class);

        FileInputFormat.setInputPaths(conf, mInputFolder);
        FileOutputFormat.setOutputPath(conf, mOutputFolder);

        JobClient.runJob(conf);

        return 0;
    }

    private void parseSimulationCount(File simulationCount) throws IOException {
        mCountDirection = new int[8];
        LineIterator iterator = FileUtils.lineIterator(simulationCount);
        try {
            while (iterator.hasNext()) {
                String[] line = iterator.nextLine().split("\t");
                String key = line[0].split("_")[1];

                if ("count".equals(key)) {
                    mCountTotal = Integer.parseInt(line[1]);
                } else {
                    mCountDirection[Integer.parseInt(key) / 45] = Integer.parseInt(line[1]);
                }

            }
        } finally {
            LineIterator.closeQuietly(iterator);
        }
    }

    private static class OutFormat extends MultipleTextOutputFormat<Text, Text> {
        @Override
        protected String generateFileNameForKeyValue(Text key, Text value, String name) {
            String valString = value.toString();
            if (valString.endsWith("t")) {
                value.set(valString.substring(0, valString.length() - 1) + ".");
                return "all.txt";
            }

            String[] val = valString.split(";");
            String direction = val[0].substring(val[0].indexOf("_") + 1);
            return direction + ".txt";
        }
    }
}
