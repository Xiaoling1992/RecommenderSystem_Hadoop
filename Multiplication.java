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
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {

	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
		enum WordList{
			Movie1,
			Movie2,
			Movie3,
			Other
		}   //class variable

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			String[] movieB_movieACorrelation= value.toString().trim().split("\t");
			context.write(new Text(movieB_movieACorrelation[0]), new Text(movieB_movieACorrelation[1]) );
			//output: [movieB; movieA=relation]

			String movieB=movieB_movieACorrelation[0];
			if (movieB.equals("10001") ){
				context.getCounter(WordList.Movie1).increment(1);
			}else if (movieB.equals("10002") ){
				context.getCounter(WordList.Movie2).increment(1);
			}else if (movieB.equals("10003") ){
				context.getCounter(WordList.Movie3).increment(1);
			}else{
				context.getCounter(WordList.Other).increment(1);
			}
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
		enum WordList1{
			User1,
			User2,
			User3,
			Other
		}   //class variable

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			String[] user_movieB_rating = value.toString().trim().split(",");
			context.write(new Text(user_movieB_rating[1]), new Text(user_movieB_rating[0]+":"+user_movieB_rating[2]) );
			//output:[movieB; user:Rating];
			String user=user_movieB_rating[0];
			if (user.equals("1") ){
				context.getCounter(WordList1.User1).increment(1);
			}else if (user.equals("2") ){
				context.getCounter(WordList1.User2).increment(1);
			}else if (user.equals("3") ){
				context.getCounter(WordList1.User3).increment(1);
			}else{
				context.getCounter(WordList1.Other).increment(1);
			}

		}
	}

	public static class AveRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t user::aveRating
			String[] movieB_userAveRating= value.toString().trim().split("\t");
			context.write(new Text(movieB_userAveRating[0]), new Text( movieB_userAveRating[1]) );
			//output:[movieB; user::aveRating];
			String user=movieB_userAveRating[1].split("::")[0];
			if (user.equals("1") ){
				context.getCounter(RatingMapper.WordList1.User1).increment(1);
			}else if (user.equals("2") ){
				context.getCounter(RatingMapper.WordList1.User2).increment(1);
			}else if (user.equals("3") ){
				context.getCounter(RatingMapper.WordList1.User3).increment(1);
			}else{
				context.getCounter(RatingMapper.WordList1.Other).increment(1);
			}
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB
			//value = <movieA=relation, movieC=relation, userA:rating, userB:rating, userA::aveRating, userB::aveRating...>
			Map<String, Double> relationMap = new HashMap<String, Double>();
			Map<String, Double> ratingMap = new HashMap<String, Double>();
			Map<String, Double> aveRatingMap= new HashMap<String, Double>();

			for (Text value:	 values) {
				if (value.toString().contains("=") ) {
					String[] movieRelation = value.toString().trim().split("=");
					relationMap.put(movieRelation[0], Double.parseDouble(movieRelation[1]));
				} else if(value.toString().contains("::")) {  //Must work with the string with "::" before working with the with ":"
					String[] userAveRating= value.toString().trim().split("::");
					aveRatingMap.put(userAveRating[0], Double.parseDouble( userAveRating[1]) );
				}else if (value.toString().contains(":") ) {
					String[] userRating = value.toString().trim().split(":");
					ratingMap.put(userRating[0], Double.parseDouble(userRating[1]) );
				}
			}

			for (Map.Entry<String, Double> relationEntry : relationMap.entrySet()) { //use entry to transverse
				String movieA = relationEntry.getKey();
				double relation = relationEntry.getValue(); //Double to double

				for (Map.Entry<String, Double> aveRatingEntry : aveRatingMap.entrySet() ) {
					String user = aveRatingEntry.getKey();
					if( ratingMap.containsKey(user) ){
						double rating=ratingMap.get(user);
						context.write(new Text(user + ":" + movieA), new DoubleWritable(rating * relation));//double->DoubleWritable
					}else{
						double aveRating=aveRatingEntry.getValue();
						context.write(new Text(user+ ":" + movieA), new DoubleWritable(aveRating* relation) );
					}

				}
			}


		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);


		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, AveRatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);
		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);
		job.setMapperClass(AveRatingMapper.class);
		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); //Why only define the output of one mapper?
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, AveRatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.waitForCompletion(true);
	}
}
