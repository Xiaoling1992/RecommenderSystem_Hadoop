import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageRatingGenerator {

    public static class AverageRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: user\t averageRating
            String[] user_aveRating=value.toString().trim().split("\t");
            if (user_aveRating.length!= 2){
                return;
            }
            context.write(new Text("UserList&MovieList"), new Text(user_aveRating[0]+ "::" +user_aveRating[1]) );
            //output: [$; user::aveRating]
        }
    }

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: movieB \t movieA=relation, there are N movieB_j
            String movieB = value.toString().trim().split("\t")[0];
            context.write(new Text("UserList&MovieList"), new Text( movieB ) );
            //output: [$; movieB]
        }
    }

    public static class AverageRatingGeneratorReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieB
            //value = <user1::rating, user2::rating, movieA1, movieA1, movieA2, movieA2...>
            List<String> user_aveRatings =new ArrayList<String>();
            Map<String, Integer> movies = new HashMap<String, Integer>();
            for (Text value:	 values) {
                if (value.toString().contains("::")) {
                    user_aveRatings.add(value.toString().trim() );
                } else{
                    movies.put(value.toString().trim(), Integer.parseInt("1") );
                }
            }

            for (Map.Entry<String, Integer> movieEntry : movies.entrySet()) { //use entry to transversal
                String movie = movieEntry.getKey();
                for (int i=0; i< user_aveRatings.size(); i++){
                    String user_aveRating= user_aveRatings.get(i);
                    context.write(new Text(movie), new Text(user_aveRating));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AverageRatingGenerator.class);

        ChainMapper.addMapper(job, AverageRatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf); //This the data flow between mappers. So the input classes needs to agree with the output classes of the last mapper.
        ChainMapper.addMapper(job, MovieMapper.class, Text.class, Text.class, Text.class, Text.class, conf);       //The input classes could be different from te input callssed in the method: which defines the input classes from the files.
        job.setMapperClass(AverageRatingMapper.class);
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(AverageRatingGeneratorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AverageRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}

