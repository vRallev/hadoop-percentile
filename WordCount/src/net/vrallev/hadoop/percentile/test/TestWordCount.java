package net.vrallev.hadoop.percentile.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.vrallev.hadoop.percentile.PercentileMapper;
import net.vrallev.hadoop.percentile.PercentileReducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {
		PercentileMapper mapper = new PercentileMapper();
		PercentileReducer reducer = new PercentileReducer();
		mapDriver = MapDriver.newMapDriver(mapper);

		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
//        Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('0','27269','1','-1','0','-1','-1',null,null);
//        Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('0','8348','1','0','0','623954','4101','632307','1123025');
		//mapDriver.withInput(new LongWritable(), new Text("Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('0','27269','1','-1','0','-1','-1',null,null);"));
		mapDriver.withInput(new LongWritable(), new Text("Insert into HS_NRW_4708_NEIGHBOUR (DIRECTION,NODE_REF,BUFFER_ZONE,DISTANCE,HAS_VEGETATION,JKI_ID,OBJART,LF08_LF_ID,LF04_BBA_ID) values ('0','8348','1,3','0','0','623954','4101','632307','1123025');"));

        for (int i = 0; i < 1000; i++) {
            mapDriver.withOutput(new Text("27270_270"), new DoubleWritable(-0.29715));
        }

//		mapDriver.withOutput(new Text("alex"), new DoubleWritable(1));

        mapDriver.runTest(false);
	}

	@Test
	public void testReducer() throws IOException {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(1));
		values.add(new DoubleWritable(1));
		reduceDriver.withInput(new Text("6"), values);
//		reduceDriver.withOutput(new Text("6"), new DoubleWritable(2));
		reduceDriver.runTest();
	}
}
