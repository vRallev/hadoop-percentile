package net.vrallev.hadoop.percentile;

import net.vrallev.hadoop.percentile.analyze.PercentileTool;
import net.vrallev.hadoop.percentile.simulate.SimulationTool;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Ralf Wondratschek
 */
public class Main {

    public static void main(String... args) throws Exception {

        long time = System.currentTimeMillis();

        if (args.length < 1) {
            System.err.println("You need pass a configuration file.");
            System.exit(1);
        }

        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(args[0]);
            props.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            IOUtils.closeQuietly(fis);
        }


        int res = 0;
        long timeSimulation = -1;
        if (Boolean.parseBoolean(props.getProperty(SimulationTool.SIMULATE, "false"))) {
            // user chose to start a simulation
            res = ToolRunner.run(new SimulationTool(props), args);
            timeSimulation = System.currentTimeMillis() - time;
        }

        if (res != 0) {
            System.exit(res);
        }

        long timePercentile = -1;
        if (Boolean.parseBoolean(props.getProperty(PercentileTool.ANALYZE, "false"))) {
            // user chose to start a simulation
            res = ToolRunner.run(new PercentileTool(props), args);
            timePercentile = System.currentTimeMillis() - time - (timeSimulation > 0 ? timeSimulation : 0);
        }

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        if (timeSimulation > 0) {
            System.out.println("Time (hh:mm:ss) needed for simulation: " + dateFormat.format(new Date(timeSimulation - 3600000)));
        }
        if (timePercentile > 0) {
            System.out.println("Time (hh:mm:ss) needed for percentile: " + dateFormat.format(new Date(timePercentile - 3600000)));
        }

        System.exit(res);
    }
}
