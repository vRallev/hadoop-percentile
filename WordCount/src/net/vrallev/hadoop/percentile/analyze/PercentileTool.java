package net.vrallev.hadoop.percentile.analyze;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

    private final Path mInputFolder;
    private final Path mOutputFolder;

    private final Path mSimulationCountFile;
    private int mCountTotal;
    private int[] mCountDirection;

    private final boolean mClearOutputFolder;

    public PercentileTool(Properties properties) {
        mInputFolder = new Path(properties.getProperty(INPUT_FOLDER));
        mOutputFolder = new Path(properties.getProperty(OUTPUT_FOLDER));

        mSimulationCountFile = new Path(properties.getProperty(SIMULATION_COUNT));

        mClearOutputFolder = Boolean.parseBoolean(properties.getProperty(CLEAR_OUTPUT_FOLDER, "false"));
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse the file to know how much simulation were calculated
        parseSimulationCount(mSimulationCountFile);

        if (mClearOutputFolder) {
            // clear output
            FileSystem.get(getConf()).delete(mOutputFolder, true);
        }

        // pass parameters to components
        getConf().setInt("count_total", mCountTotal);
        for (int i = 0; i < mCountDirection.length; i++) {
            getConf().setInt("count_" + (i * 45), mCountDirection[i]);
        }

        JobConf conf = new JobConf(getConf(), PercentileTool.class);
        conf.setJobName("Analyze");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(PercentileMapper.class);
        conf.setReducerClass(PercentileReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(OutFormat.class);

        FileInputFormat.setInputPaths(conf, mInputFolder);
        FileOutputFormat.setOutputPath(conf, mOutputFolder);

        JobClient.runJob(conf);

        return 0;
    }

    /**
     * Parses the simulation count file.
     *
     * @param simulationCount {@link Path} to the file, which contains the simulation count.
     * @throws IOException If an IO error occurs.
     */
    private void parseSimulationCount(Path simulationCount) throws IOException {
        mCountDirection = new int[8];

        FileSystem fs = FileSystem.get(getConf());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(simulationCount)));
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                String[] lineSplit = line.split("\t");
                String key = lineSplit[0].split("_")[1];

                if ("count".equals(key)) {
                    mCountTotal = Integer.parseInt(lineSplit[1]);
                } else {
                    mCountDirection[Integer.parseInt(key) / 45] = Integer.parseInt(lineSplit[1]);
                }

            }
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    private static class OutFormat extends MultipleTextOutputFormat<Text, Text> {
        @Override
        protected String generateFileNameForKeyValue(Text key, Text value, String name) {
            String valString = value.toString();

            // "t" is used as suffix to differentiate between all simulations and the directions
            if (valString.endsWith("t")) {
                value.set(valString.substring(0, valString.length() - 1));
                return "all.txt";
            }

            String[] val = valString.split(";");
            String direction = val[0].substring(val[0].indexOf("_") + 1);
            return direction + ".txt";
        }
    }
}
