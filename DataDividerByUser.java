import java.io.IOException;

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

public class DataDividerByUser {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input user,movie,rating
			//merge all movies for each single user
			//outputkey u_j
			String[] user_movie_rating=value.toString().trim().split(",");
			String outputKey= user_movie_rating[0];
			String outputValue= user_movie_rating[1]+":"+ user_movie_rating[2];  //Use : to divide to Strings
			context.write(new IntWritable(Integer.parseInt(outputKey) ), new Text(outputValue) );
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = userID
			//outputValue = list of movies
			StringBuilder sb=new StringBuilder();  //String is immutable in Java, so we need to use String builder to
			for (Text value: values){
				sb.append(","+ value.toString() ); //Convert the iterable to be a String.
			}
            context.write(key, new Text(sb.toString().replaceFirst(",", "") ) );

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(DataDividerByUser.class);

		job.setInputFormatClass(TextInputFormat.class); //read the input by text format: files.
		job.setOutputFormatClass(TextOutputFormat.class); //write the output by text format: to files.
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));  //Input path
		TextOutputFormat.setOutputPath(job, new Path(args[1])); //Output path

		job.waitForCompletion(true);
	}

}
