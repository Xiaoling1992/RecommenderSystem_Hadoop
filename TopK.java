import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopK {
    public static class PairComparator implements Comparator<Pair>{

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(Pair s1, Pair s2) {
            if (s1.rating < s2.rating)
                return -1;
            else if (s1.rating > s2.rating)
                return 1; //swaping is needed, ascending
            return 0;
        }
    }

    public static class Pair{
        public String movie;
        public double rating;

        public Pair(String movie, double rating){
            this.movie=movie;
            this.rating=rating;
        }
        public String getName(){
            return this.movie;
        }
        public double getRating(){
            return this.rating;
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
            // map method, inheritance?
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: user,movie,rating
            String[] user_movieB_rating = value.toString().trim().split(",");
            context.write(new Text(user_movieB_rating[0]), new Text(user_movieB_rating[1]+"="+user_movieB_rating[2]) );
            //output:[user_j; movie_i=Rating]
        }
    }

    public static class PredictMapper extends Mapper<LongWritable, Text, Text, Text> {
        // map method, inheritance?
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: [user:movie; rating]
            String[] user_movieB_rating = value.toString().trim().split("\t");
            String user= user_movieB_rating[0].trim().split(":")[0];
            String movie=user_movieB_rating[0].trim().split(":")[1];
            context.write(new Text(user), new Text(movie+"="+user_movieB_rating[1]) );
            //output:[user_j; movie_i=predictedRating];
        }
    }



    public static class TopKReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int KMovies;
        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            KMovies = conf.getInt("KMovies", 10);
        }

        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //[user_j: <movie_i=rating, movie_i=predictedRating>]
            Map<String, Double> unratedMoviesMap = new HashMap<String, Double>();
            for (Text value: values){
                String[] movie_rating= value.toString().trim().split("=");
                String movie= movie_rating[0];
                if ( unratedMoviesMap.containsKey( movie) ){
                    unratedMoviesMap.remove(movie);
                }else{
                    unratedMoviesMap.put(movie, Double.parseDouble( movie_rating[1] ) );
                      }
            }

            PriorityQueue<Pair> pq = new
                    PriorityQueue<Pair>(KMovies, new PairComparator() );

            //traverse the HashMap, find out the TopK;
            for (Map.Entry<String, Double> unratedMoviesEntry : unratedMoviesMap.entrySet() ) {
                String unratedMovie = unratedMoviesEntry.getKey();
                double predictedRating = unratedMoviesEntry.getValue();
                if (pq.size() < KMovies) {
                    pq.add(new Pair(unratedMovie, predictedRating) );
                } else {
                    if (predictedRating > pq.peek().getRating() ) {//compare with the most priority movie
                        pq.poll();
                        pq.add(new Pair(unratedMovie, predictedRating) );
                    }
                }
            }

            //Write to database
            DecimalFormat df = new DecimalFormat("#.0000");
            String[] movies= {"NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie", "NoMovie"};
            int i_movies=0;
            while (pq.size()> 0 && i_movies< KMovies){ //end: no element in pq or write KMovies into movies
                Pair user_movie=pq.poll();
                movies[i_movies]=user_movie.getName()+":"+df.format( user_movie.getRating() );
                i_movies= i_movies+1;   //iteration
            }

            context.write(new DBOutputWritable(key.toString(), movies),  NullWritable.get() );

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); //read by line
        conf.set("KMovies", args[2] );
        DBConfiguration.configureDB(conf,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.246:3306/test",  //test is the database to write table output
                "root",
                "password");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TopK.class);
        job.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar") );

        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PredictMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
        //ChainMapper, input class need to the same with the output class of the last ChainMapper.
        job.setMapperClass(RatingMapper.class);
        job.setMapperClass(PredictMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); //Why only define the output of one mapper?
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PredictMapper.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "predictedRating",
                new String[] {"user", "movie0", "movie1", "movie2", "movie3", "movie4", "movie5", "movie6", "movie7", "movie8", "movie9"});


        job.waitForCompletion(true);
    }
}
