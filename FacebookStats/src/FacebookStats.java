import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.FacebookClient;
import com.restfb.Version;
import com.restfb.types.Page;
import com.restfb.types.Post;

public class FacebookStats {

	public static class Map extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://127.0.0.1:9000/"),
				conf);
		Path file = new Path("hdfs://127.0.0.1:9000/in/posts.txt");

		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				hdfs.append(file)));
		FacebookClient fClient = new DefaultFacebookClient(
				"CAACEdEose0cBAAJTMhDMHTO9AE7TOh0a5HASN9o6VE3dSjWZBykqG1G1zQ0zxkjoN1iIEObrKurFjuEMAjkTiTuZBms57YSOHMVFSELBWCDZC0zVgXEmHmrhbNKBcYLRnLDo1gLEeN0BZAmqyFX9JDTfI7VTrYp0Pz3Qa9F1pgEyZCNfPq075qxIVFjPcAMAeqQdo2gxmswZDZD",
				Version.VERSION_2_5);
		Page page = fClient.fetchObject("me", Page.class);
		Connection<Post> pageFeed = fClient.fetchConnection(page.getId()
				+ "/feed", Post.class);
		for (List<Post> feed : pageFeed) {
			for (Post post : feed) {
				if (post.getMessage() == null)
					br.write("Null\n");
				else
					br.write(post.getMessage() + "\n");
			}
		}
		
		br.close();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "FbStats");

		job.setJarByClass(FacebookStats.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/in"));
		
		if (hdfs.exists(new Path("hdfs://127.0.0.1:9000/out")))
			hdfs.delete(new Path("hdfs://127.0.0.1:9000/out"), true);
		
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://127.0.0.1:9000/out"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		job.submit();
		
		hdfs.close();

	}
}