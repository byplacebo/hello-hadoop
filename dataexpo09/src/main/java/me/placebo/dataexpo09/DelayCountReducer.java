package me.placebo.dataexpo09;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 임형태
 * @since 2016.04.21
 */
public class DelayCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Log LOGGER = LogFactory.getLog(DelayCountReducer.class);
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values)
            sum += value.get();
        result.set(sum);
        LOGGER.fatal("Key: " + key + ", Result : " + result);
        context.write(key, result);
    }
}
