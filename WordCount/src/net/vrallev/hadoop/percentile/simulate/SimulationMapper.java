package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimulationMapper implements Mapper<LongWritable, Text, DoubleWritable, Text> {

    public static final String DIRECTION = "DIRECTION";
    public static final String NODE_REF = "NODE_REF";
    public static final String DISTANCE = "DISTANCE";
    public static final String HAS_VEGETATION = "HAS_VEGETATION";

    public static final DoubleWritable COUNT_KEY = new DoubleWritable(Double.MAX_VALUE);

    private Simulation mSimulation;
    private int mNumberOfSimulations;
    private int mNumbersAfterComma;

    public void configure(JobConf conf) {
        String clazz = conf.get(SimulationTool.SIMULATION_CLASS, SimulationDefault.class.getName());
        try {
            mSimulation = (Simulation) Class.forName(clazz).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            // ignore, job will fail later
        }

        mNumberOfSimulations = Integer.parseInt(conf.get(SimulationTool.NUMBER_OF_SIMULATIONS));
        mNumbersAfterComma = Integer.parseInt(conf.get(SimulationTool.NUMBERS_AFTER_COMMA));
    }

    public void close() throws IOException {

    }

    /*
     * Avoid creation for a every map call.
     */
    private Text mValue = new Text();
    private Text mTextValue = new Text();
    private DoubleWritable mSimulationResult = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
        String[] tokens = parse(key, value);

        String direction = getValue(tokens, DIRECTION);
        String nodeRef = getValue(tokens, NODE_REF);
        String distance = getValue(tokens, DISTANCE);
        String hasVegetation = getValue(tokens, HAS_VEGETATION);

        mValue.set(nodeRef + "_" + direction);

        for (int i = 0; i < mNumberOfSimulations; i++) {
            mSimulationResult.set(round(mSimulation.simulate(distance, hasVegetation), mNumbersAfterComma));
            output.collect(mSimulationResult, mValue);
        }

        mTextValue.set(direction);
        output.collect(COUNT_KEY, mTextValue);
    }

    public static String[] parse(LongWritable key, Text value) throws IllegalArgumentException {

        // String line = "Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('270','27270','1','5,1','0','632755','4101','640580','1227424');";
        String[] tokens = value.toString().split(" ");

        if (tokens.length != 6) {
            throw new IllegalArgumentException("Wrong data format. Expected 6 tokens per line, found at line " + key.get() + " " + tokens.length + " token.");
        }

        String keyToken = tokens[3];
        String valueToken = tokens[5];

        if (!keyToken.startsWith("(") || !keyToken.endsWith(")")) {
            throw new IllegalArgumentException("Wrong key token format, found at line " + key.get() + ", " + keyToken);
        }

        if (!valueToken.startsWith("(") || !valueToken.endsWith(");")) {
            throw new IllegalArgumentException("Wrong value token format, found at line " + key.get() + ", " + valueToken);
        }

        keyToken = keyToken.substring(1, keyToken.length() - 1);
        String[] keys = keyToken.split(",");

        valueToken = valueToken.substring(1, valueToken.length() - 2);
        String[] values = valueToken.split("'");
        List<String> valueList = new ArrayList<String>();
        for(String s : values) {
            if (s.isEmpty() || ",".equals(s)) {
                continue;
            }

            if (s.contains("null")) {
                String[] split = s.split(",");
                for(String s2 : split) {
                    if (!s2.isEmpty()) {
                        valueList.add(s2);
                    }
                }
                continue;
            }

            valueList.add(s);
        }

        values = valueList.toArray(new String[valueList.size()]);

        if (keys.length != values.length) {
            throw new IllegalArgumentException("Key and value count differ, found at line " + key.get() + ", " + keyToken + " | " + valueToken);
        }

        String[] result = new String[keys.length * 2];
        System.arraycopy(keys, 0, result, 0, keys.length);
        System.arraycopy(values, 0, result, result.length / 2, values.length);

        return result;
    }

    private String getValue(String[] tokens, String key) {
        int index = -1;
        for (int i = 0; i < tokens.length / 2; i++) {
            if (tokens[i].equals(key)) {
                index = i;
                break;
            }
        }

        if (index < 0) {
            return null;
        } else {
            return tokens[tokens.length / 2 + index];
        }
    }

    private static double round(double number, int countAfterComma) {
        long val = Math.round(number * Math.pow(10, countAfterComma));
        return val / Math.pow(10, countAfterComma);
    }
}