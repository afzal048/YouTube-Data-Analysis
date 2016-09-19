import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Map extends Mapper<LongWritable, Text, Text,
IntWritable> {
	private Text category = new Text();
	private final static IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context )
	throws IOException, InterruptedException {
	String line = value.toString();
	String str[]=line.split("\t");
	if(str.length > 5){
		category.set(str[3]);
		}
	context.write(category, one);
	}
}
