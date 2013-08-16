package net.vrallev.hadoop.percentile.simulate.test;

import net.vrallev.hadoop.percentile.simulate.Simulation;

/**
 * Used for testing the {@link Simulation} interface.
 *
 * @author Ralf Wondratschek
 */
@Deprecated
public class SimulationTest implements Simulation {

    @Override
    public double simulate(String distance, String hasVegetation) {
        return 42.0;
    }
}
