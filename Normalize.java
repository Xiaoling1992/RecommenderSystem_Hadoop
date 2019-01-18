import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value: movieA: movieB\t relation
            //outputKey: movieA
            //outputValue: movieB=relation

            String[] movie_relation=value.toString().trim().split("\t");
            if(movie_relation.length != 2) { //Check after reading the text.
                return;
            }
            String movieA= movie_relation[0].trim().split(":")[0];
            String movieB= movie_relation[0].trim().split(":")[1];
            context.write(new Text(movieA), new Text(movieB+"="+movie_relation[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //sum -> denominator
            //context write -> transpose
            //Iterable: Don't support more than once iteration
            int sum=0;
            Map<String, Integer> map=new HashMap<String, Integer>();
            for (Text value: values){  //Variable contains data type and variable name
                String[] movieB_frequency=value.toString().split("="); //How to name a variable
                String movieB=movieB_frequency[0];
                int frequency= Integer.parseInt(movieB_frequency[1]);
                sum+=frequency;
                map.put(movieB, frequency);
            }

            for( Map.Entry<String, Integer> entry: map.entrySet()){
                String outputKey=entry.getKey();
                double correlation=(double)entry.getValue()/sum;
                String outputValue=key.toString().trim()+"="+correlation;
                context.write(new Text(outputKey), new Text(outputValue) );
            }

            }
        }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
