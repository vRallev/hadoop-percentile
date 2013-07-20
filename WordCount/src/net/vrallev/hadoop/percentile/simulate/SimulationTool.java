package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.File;

/**
 * @author Ralf Wondratschek
 */
public class SimulationTool extends Configured implements Tool {

    public static final String CLEAR_OUTPUT_FOLDER = "clearSimulationOutput";
    public static final String NUMBERS_AFTER_COMMA = "numbersAfterComma";
    public static final String NUMBER_OF_SIMULATIONS = "numberOfSimulations";

    @Override
    public int run(String[] args) throws Exception {
        Simulation simulation = null;

        // start with 2, 0 is input folder, 1 is output folder
        for (int i = 2; i < args.length; i++) {
            if (CLEAR_OUTPUT_FOLDER.equalsIgnoreCase(args[i])) {
                File file = new File(args[1]);
                if (file.exists() && file.isDirectory()) {
                    FileUtil.fullyDelete(file);
                }

            } else if (NUMBERS_AFTER_COMMA.equalsIgnoreCase(args[i])) {
                i++;
                int numbersAfterComma = Integer.parseInt(args[i]); // try to parse
                getConf().set(NUMBERS_AFTER_COMMA, String.valueOf(numbersAfterComma));

            } else if (NUMBER_OF_SIMULATIONS.equalsIgnoreCase(args[i])) {
                i++;
                int numberOfSimulations = Integer.parseInt(args[i]); // try to parse
                getConf().set(NUMBER_OF_SIMULATIONS, String.valueOf(numberOfSimulations));

            } else if (args[i] != null) {
                try {
                    simulation = (Simulation) Class.forName(args[i]).newInstance();
                } catch (Exception e) {
                    // ignore
                }
            }
        }

        if (simulation != null) {
            getConf().set(Simulation.class.getSimpleName(), simulation.getClass().getName());
        }

        JobConf conf = new JobConf(getConf(), SimulationTool.class);
        conf.setJobName("Simulation");

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(SimulationMapper.class);
        // conf.setCombinerClass(SimulationReducer.class);
        conf.setReducerClass(SimulationReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        return 0; // it will throw an Exception, if it crashes, so always return zero
    }

}
