package me.placebo.dataexpo09;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

/**
 * @author 임형태
 * @since 2016.04.21
 */
public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Log LOGGER = LogFactory.getLog(DelayCountMapper.class);
    private final static IntWritable outputValue = new IntWritable(1);
    private String workType;
    private Text outputKey = new Text();

    @Override
    public void setup(Context context) {
        workType = Optional.ofNullable(context.getConfiguration().get("workType")).orElse("departure");
        LOGGER.fatal("WorkType: " + workType);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] columns = value.toString().split(",");
            if (columns.length > 0) {
                try {
                    if (workType.equals("departure")) {
                        if (!columns[15].equals("NA")) {
                            int depDelayTime = Integer.parseInt(columns[15]);
                            if (depDelayTime > 0) {
                                outputKey.set(columns[0] + "," + columns[1]);
                                context.write(outputKey, outputValue);
                            }
                        }
                    } else if (workType.equals("arrival")) {
                        if (!columns[14].equals("NA")) {
                            int arrDelayTime = Integer.parseInt(columns[14]);
                            if (arrDelayTime > 0) {
                                outputKey.set(columns[0] + "," + columns[1]);
                                context.write(outputKey, outputValue);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
