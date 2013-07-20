package net.vrallev.hadoop.percentile;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.File;
import java.io.IOException;

public final class PercentileMain {

	private PercentileMain() {
		// Do nothing
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(PercentileMain.class);
		conf.setJobName("Percentile");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(PercentileMapper.class);
//		conf.setCombinerClass(PercentileReducer.class);
		conf.setReducerClass(PercentileReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

        // TODO: remove
        File file = new File(args[1]);
        if (file.exists() && file.isDirectory()) {
            FileUtil.fullyDelete(file);
        }

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		/*
		 * JobClient Checking the input and output specifications of the job. Computing the InputSplits for the job. Setup the requisite accounting information
		 * for the DistributedCache of the job, if necessary. Copying the job's jar and configuration to the map-reduce system directory on the distributed
		 * file-system. Submitting the job to the JobTracker and optionally monitoring it's status.
		 */
		JobClient.runJob(conf);
	}

}
