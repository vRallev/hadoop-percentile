package net.vrallev.hadoop.percentile.simulate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class SimulationReducer implements Reducer<DoubleWritable, Text, Text, Text> {

    private int mNumbersAfterComma;
    private int mNumberOfSimulations;

    @Override
	public void configure(JobConf conf) {
        mNumberOfSimulations = Integer.parseInt(conf.get(SimulationTool.NUMBER_OF_SIMULATIONS, String.valueOf(SimulationMapper.DEFAULT_NUMBER_OF_SIMULATIONS)));
        mNumbersAfterComma = Integer.parseInt(conf.get(SimulationTool.NUMBERS_AFTER_COMMA, String.valueOf(SimulationMapper.DEFAULT_NUMBERS_AFTER_COMMA)));
    }

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        if (key.get() == SimulationMapper.COUNT_KEY.get()) {
            int[] count = new int[8];
            while (values.hasNext()) {
                int index = Integer.parseInt(values.next().toString()) / 45;
                count[index]++;
            }

            int total = 0;
            for (int i = 0; i < count.length; i++) {
                count[i] *= mNumberOfSimulations;
                output.collect(new Text("total_" + (i * 45)), new Text(String.valueOf(count[i])));
                total += count[i];
            }

            output.collect(new Text("total_count"), new Text(String.valueOf(total)));
            return;
        }

        Text keyText = keyToText(key, mNumbersAfterComma);
//        Text[] texts = parseValues(values);

//        for (Text value : texts) {
//            output.collect(keyText, value);
//        }

        while (values.hasNext()) {
            output.collect(keyText, values.next());
        }
    }

    private static Text keyToText(DoubleWritable key, int numbersAfterComma) {
        double val = key.get();
        StringBuilder builder = new StringBuilder(String.valueOf(val));
        if (val >= 0) {
            builder.insert(0, " ");
        }

        while(builder.length() - builder.indexOf(".") - 1 < numbersAfterComma) {
            builder.append('0');
        }
        return new Text(builder.toString());
    }

    private static Text[] parseValues(Iterator<Text> values) {
        Map<Integer, List<String>> coll = new HashMap<Integer, List<String>>();
        while(values.hasNext()) {
            String string = values.next().toString();
            String[] split = string.split("_");
            Integer key = Integer.valueOf(split[1]);
            List<String> list = coll.get(key);
            if (list == null) {
                list = new ArrayList<String>();
                coll.put(key, list);
            }
            list.add(string);
        }

        Text[] res = new Text[coll.size()];
        int j = 0;
        for (Integer key : coll.keySet()) {
            StringBuilder builder = new StringBuilder();
            builder.append('(');

            List<String> valueList = coll.get(key);
            for (int i = 0; i < valueList.size(); i++) {
                builder.append('\'').append(valueList.get(i)).append('\'');
                if (i + 1 < valueList.size()) {
                    builder.append(',');
                }

            }

            builder.append(");");
            res[j] = new Text(builder.toString());
            j++;
        }

        return res;
    }
}
