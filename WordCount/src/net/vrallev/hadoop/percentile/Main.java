package net.vrallev.hadoop.percentile;

import net.vrallev.hadoop.percentile.simulate.SimulationTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Ralf Wondratschek
 */
public class Main {

    public static void main(String... args) throws Exception {

        // TODO: print possible parameters

        int res = ToolRunner.run(new SimulationTool(), args);
        System.exit(res);
    }

}
