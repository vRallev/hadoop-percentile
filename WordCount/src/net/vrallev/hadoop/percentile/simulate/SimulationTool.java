package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.Properties;

/**
 * @author Ralf Wondratschek
 */
public class SimulationTool extends Configured implements Tool {

    public static final String SIMULATE = "simulate";
    public static final String INPUT_FOLDER = "input_simulate";
    public static final String OUTPUT_FOLDER = "output_simulate";
    public static final String SIMULATION_CLASS = "simulationClass";
    public static final String NUMBERS_AFTER_COMMA = "numbersAfterComma";
    public static final String NUMBER_OF_SIMULATIONS = "numberOfSimulations";
    public static final String CLEAR_OUTPUT_FOLDER = "clearOutputFolder_simulate";

    private Path mInputFolder;
    private Path mOutputFolder;

    private Simulation mSimulation;

    private int mNumberOfSimulations;
    private int mNumbersAfterComma;
    private boolean mClearOutputFolder;

    public SimulationTool(Properties properties) throws Exception {
        mInputFolder = new Path(properties.getProperty(INPUT_FOLDER));
        mOutputFolder = new Path(properties.getProperty(OUTPUT_FOLDER));

        mSimulation = (Simulation) Class.forName(properties.getProperty(SIMULATION_CLASS, SimulationDefault.class.getName())).newInstance();

        mNumberOfSimulations = Integer.parseInt(properties.getProperty(NUMBER_OF_SIMULATIONS, "10000"));
        mNumbersAfterComma = Integer.parseInt(properties.getProperty(NUMBERS_AFTER_COMMA, "5"));
        mClearOutputFolder = Boolean.parseBoolean(properties.getProperty(CLEAR_OUTPUT_FOLDER, "false"));
    }

    @Override
    public int run(String[] args) throws Exception {

        // pass the parameters to the components
        getConf().set(SIMULATION_CLASS, mSimulation.getClass().getName());
        getConf().setInt(NUMBER_OF_SIMULATIONS, mNumberOfSimulations);
        getConf().setInt(NUMBERS_AFTER_COMMA, mNumbersAfterComma);

        if (mClearOutputFolder) {
            // clear output
            FileSystem.get(getConf()).delete(mOutputFolder, true);
        }

        JobConf conf = new JobConf(getConf(), SimulationTool.class);
        conf.setJobName("Simulation");

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(SimulationMapper.class);
        conf.setReducerClass(SimulationReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(OutFormat.class);

        FileInputFormat.setInputPaths(conf, mInputFolder);
        FileOutputFormat.setOutputPath(conf, mOutputFolder);

        JobClient.runJob(conf);

        return 0; // it will throw an Exception, if it crashes, so always return zero, we don't care here anymore
    }

    private static class OutFormat extends MultipleTextOutputFormat<Text, Text> {
        @Override
        protected String generateFileNameForKeyValue(Text key, Text value, String name) {
            // "total_" is used as prefix to differentiate between all simulations and the directions
            if (key.toString().startsWith("total_")) {
                return "total_simulation.txt";
            }

            String[] split = value.toString().split("'");
            for (String s : split) {
                if (s.contains("_")) {
                    int startIndex = s.indexOf("_") + 1;
                    return "results/" + s.substring(startIndex, s.indexOf(";", startIndex)) + ".txt";
                }
            }

            return super.generateFileNameForKeyValue(key, value, name);
        }
    }
}
