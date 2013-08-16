package net.vrallev.hadoop.percentile.simulate;

/**
 * @author Ralf Wondratschek
 */
public interface Simulation {

    /**
     * @param distance The DISTANCE value of the line.
     * @param hasVegetation The HAS_VEGETATION value of the line.
     * @return The simulation result.
     */
    public double simulate(String distance, String hasVegetation);

}
