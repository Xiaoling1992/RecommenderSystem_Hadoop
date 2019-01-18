import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageRating{ //Class; classMethods; keyword;
    public static class AverageRatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //value = userid\t movie1:rating, movie2:rating,....
            //[1,2,3] -> 1,1 1,2 1,3 2,1 2,2 2,3...
            String[] user_movieRating=value.toString().trim().split("\t");  //user
            String user=user_movieRating[0];

            if (user_movieRating.length !=2){
                return;
            }

            String[] movie_rating=user_movieRating[1].split(",");  //array of rating
            for(int i=0; i<movie_rating.length; i++){
                double rating= Double.parseDouble(movie_rating[i].trim().split(":")[1] ); //rating
                context.write(new Text(user), new DoubleWritable(rating) );
                //output: [usr; rating]
            }
        }
    }

    public static class AverageRatingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            //[user; <rating1, rating2, rating3>]

            double sum = 0;  //Accumulative variable, initiate in the beginning
            int N=0;

            for (DoubleWritable value: values) {
                sum += value.get();
                N+= 1;
            }
            context.write(key, new DoubleWritable(sum/N) );
            //ouput [user; aveRating]
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AverageRating.class);

        job.setMapperClass(AverageRatingMapper.class);    //Mapper and Reducer.
        job.setReducerClass(AverageRatingReducer.class);

        job.setInputFormatClass(TextInputFormat.class);   //Input, Output directory
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);     //Input, output format
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}
