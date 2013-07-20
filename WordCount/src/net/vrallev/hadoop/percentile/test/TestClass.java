package net.vrallev.hadoop.percentile.test;

import net.vrallev.hadoop.percentile.PercentileMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TestClass {

    public static void main(String[] args) {
        /*

        String line = "Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('270','27270','1','5,1','0','632755','4101','640580','1227424');";
        String[] tokens = line.split(" ");

        if (tokens.length != 6) {
			//throw new IllegalArgumentException("Wrong data format. Expected 6 tokens per line, found at line " + key.get() + " " + tokens.length + " token.");
        }

        String keyToken = tokens[3];
        String valueToken = tokens[5];

        if (!keyToken.startsWith("(") || !keyToken.endsWith(")")) {
            // TODO: exception
        }

        if (!valueToken.startsWith("(") || !valueToken.endsWith(");")) {
            // TODO: exception
        }

        keyToken = keyToken.substring(1, keyToken.length() - 1);
        valueToken = valueToken.substring(1, valueToken.length() - 2);
        valueToken = valueToken.replaceAll("[']", "");

        String[] keys = keyToken.split(",");
        String[] values = valueToken.split(",");

        if (keys.length != values.length) {
            // TODO: exception
        }
        */

        PercentileMapper mapper = new PercentileMapper();

        String line = "Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('270','27270','1','5,1','0','632755','4101','640580','1227424');";

        try {
            mapper.map(new LongWritable(2), new Text(line), null, null);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

}
