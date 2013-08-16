package net.vrallev.hadoop.percentile.simulate;

import java.util.Random;

/**
 * Default simulation, which uses the Gaussian distribution and ignores the parameters.
 *
 * @author Ralf Wondratschek
 */
public class SimulationDefault implements Simulation {

    protected final Random mRandom;

    public SimulationDefault() {
        mRandom = new Random();
    }

    /**
     * Ignores the parameters.
     *
     * @param distance The DISTANCE value of the line.
     * @param hasVegetation The HAS_VEGETATION value of the line.
     * @return A Gaussian distribution value.
     */
    @Override
    public double simulate(String distance, String hasVegetation) {
        return mRandom.nextGaussian();
    }
}
