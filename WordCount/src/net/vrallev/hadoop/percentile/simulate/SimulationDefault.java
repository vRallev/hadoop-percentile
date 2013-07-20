package net.vrallev.hadoop.percentile.simulate;

import java.util.Random;

/**
 * @author Ralf Wondratschek
 */
public class SimulationDefault implements Simulation {

    protected final Random mRandom;

    public SimulationDefault() {
        mRandom = new Random();
    }

    @Override
    public double simulate(String distance, String hasVegetation) {
        return mRandom.nextGaussian();
    }
}
