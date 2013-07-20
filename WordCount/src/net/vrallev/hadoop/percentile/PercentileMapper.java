package net.vrallev.hadoop.percentile;

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
import java.util.Random;

public class PercentileMapper implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public static final String DIRECTION = "DIRECTION";
    public static final String NODE_REF = "NODE_REF";
    public static final String DISTANCE = "DISTANCE";
    public static final String HAS_VEGETATION = "HAS_VEGETATION";

    private static final int NUMBER_OF_SIMULATIONS = 10;
    private static final int NUMBERS_AFTER_COMMA = 5;

    //private final static IntWritable ONE = new IntWritable(1);

    public void configure(JobConf conf) {

    }

    public void close() throws IOException {

    }

    /*
     * To cut down on object creation you create a single Text object, which you will reuse
     */
    //private Text word = new Text();

    private Text mKey = new Text();
    private DoubleWritable mSimulationResult = new DoubleWritable();

    /*
     * This map- method is called once per input line; map tasks are run in parallel over subsets of the input files
     */
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        // Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('270','27270','1','5,1','0','632755','4101','640580','1227424');

        String[] tokens = parse(key, value);

        String direction = getValue(tokens, DIRECTION);
        String nodeRef = getValue(tokens, NODE_REF);
        String distance = getValue(tokens, DISTANCE);
        String hasVegetation = getValue(tokens, HAS_VEGETATION);

        mKey.set(nodeRef + "_" + direction);

        for (int i = 0; i < NUMBER_OF_SIMULATIONS; i++) {
            mSimulationResult.set(round(simulate(distance, hasVegetation), NUMBERS_AFTER_COMMA));
            output.collect(mKey, mSimulationResult);
        }

        /*
        Percentile percentile = new Percentile(100);
        percentile.evaluate()
        */

        // Your value contains an entire line from your file
//		String line = value.toString();
//		// You tokenize the line using StringTokenizer
//		StringTokenizer tokenizer = new StringTokenizer(line);
//
//		while (tokenizer.hasMoreTokens()) {
//			word.set(tokenizer.nextToken().toLowerCase());
//			// Thats the map-output (Text/IntWritable)
//			output.collect(word, ONE);
//		}
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

        // ('0','27269','1','-1','0','-1','-1',null,null);
        //if (!valueToken.startsWith("('") || !valueToken.endsWith("');")) {
        //    throw new IllegalArgumentException("Wrong value token format, found at line " + key.get() + ", " + valueToken);
        //}
        if (!valueToken.startsWith("(") || !valueToken.endsWith(");")) {
            throw new IllegalArgumentException("Wrong value token format, found at line " + key.get() + ", " + valueToken);
        }

        keyToken = keyToken.substring(1, keyToken.length() - 1);
        String[] keys = keyToken.split(",");

        // '0','27269','1','-1','0','-1','-1',null,null
        // '270','27270','1','5,1','0','632755','4101','640580','1227424'
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

    public String getValue(String[] tokens, String key) {
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

    private Random mRandom = new Random();

    public double simulate(String distance, String hasVegetation) {
        // TODO: delete, used for unit testing
//        mRandom.setSeed(distance.hashCode() + hasVegetation.hashCode());
        return mRandom.nextGaussian();
    }

    public double round(double number, int countAfterComma) {
        long val = Math.round(number * Math.pow(10, countAfterComma));
        return val / Math.pow(10, countAfterComma);
    }
}
