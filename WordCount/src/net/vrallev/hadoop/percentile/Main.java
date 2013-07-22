package net.vrallev.hadoop.percentile;

import net.vrallev.hadoop.percentile.simulate.SimulationTool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

/**
 * @author Ralf Wondratschek
 */
public class Main {

    public static void main(String... args) throws Exception {

        // TODO: print possible parameters

        int res = ToolRunner.run(new SimulationTool(), args);
//        System.exit(res);

        PercentileParser percentileParser = new PercentileParser(new File("/Users/Ralf/Desktop/output/total_simulation.txt"), new File("/Users/Ralf/Desktop/output/results/0.txt"));
        percentileParser.print(System.out);
    }

}
