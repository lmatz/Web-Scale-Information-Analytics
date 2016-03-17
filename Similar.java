import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Similar {

    // Top K 
    static int K=10;

    public static class Phase1Mapper
    extends Mapper<LongWritable, Text, Text, NullWritable>{

        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String line = value.toString();
            int index = line.indexOf(':');
            if ( index == -1 ) {
                return;
            }

            ArrayList<String> followers = new ArrayList<String>();
            StringTokenizer stringTokenizer = new StringTokenizer(line.substring(index+1)," ");
            while (stringTokenizer.hasMoreTokens()) {
                followers.add(stringTokenizer.nextToken());
            }

            int listSize = followers.size();
            for(int i=0;i<listSize;i++) {
                for(int j=0;j<listSize;j++) {
                    if(j == i) {
                                continue;
                    }
                        context.write(new Text(followers.get(i)+" "+followers.get(j)), NullWritable.get());
                }
            }

        }
    }

    public static class Phase1Reducer
    extends Reducer<Text,NullWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            int sum = 0;
            for (NullWritable val : values) {
                sum += 1;
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static class Phase2Mapper
    extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String line = value.toString();

            int index;
            index = line.indexOf(' ');
            if( index == -1) {
                return;
            }

            int index2;
            index2 = line.indexOf('\t');
            if( index2 == -1) {
                return;
            }

            String me = line.substring(0, index);
            String friend = line.substring(index+1, index2);
            String numberOfCommonFollowees = line.substring(index2+1);
            
                context.write(new Text(me), new Text(friend + ' ' + numberOfCommonFollowees));
            
        }
    }

    public static class Phase2Reducer
    extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context
                ) throws IOException, InterruptedException {
            
            ArrayList<AbstractMap.SimpleEntry<Integer,Integer>> topK  = new ArrayList<>();
            
            for (Text val : values) {
                String line = val.toString();
                
                int index;
                index = line.indexOf(' ');
                if( index == -1) {
                    return;
                }
                
                int pairKey = Integer.parseInt(line.substring(0, index));
                int pairValue = Integer.parseInt(line.substring(index+1));
                
                SimpleEntry<Integer,Integer> pair = new SimpleEntry<>(pairKey,pairValue);

                if( topK.isEmpty() ) {
                    topK.add(0,pair);
                }
                else {
                    int i;
                    for(i = 0; i < Math.min(topK.size(), K); i ++) {
                        if( topK.get(i).getValue() < pair.getValue() ||
                                (topK.get(i).getValue() == pair.getValue() && topK.get(i).getKey() > pair.getKey()) ) {
                            topK.add(i,pair);
                            while(topK.size() > K) {
                                topK.remove(topK.size()-1);
                            }
                            break;
                        }
                    }
                    // if the list is not full and the current pair has the least number of common followees
                    // we need to add it to the end of the list
                    if( i == topK.size() && i < K) {
                        topK.add(pair);
                    }
                }
            }
            String result = "";
            for(int i = 0; i < topK.size(); i ++){
                result += Integer.toString(topK.get(i).getKey());
                if( i != topK.size()-1 ) {
                    result += " ";
                }
            }
            context.write(key, new Text(result));
        }
    }



    public static void main(String[] args) throws Exception {
        
        K = Integer.parseInt(args[3]);


        // phase1
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.compress.map.output", true);  
            conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);

        JobConf jobConf = new JobConf(conf);
        jobConf.setJobName("Phase1");
        jobConf.setNumReduceTasks(9);

        Job job = Job.getInstance(jobConf, "phase1");
        job.setJarByClass(Similar.class);

        job.setMapperClass(Phase1Mapper.class);
        job.setReducerClass(Phase1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setCompressOutput(job, true);  
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); 

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);


        // phase2
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.output.textoutputformat.separator", ":");
        conf2.setBoolean("mapred.compress.map.output", true);  
            conf2.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class); 
            
        JobConf jobConf2 = new JobConf(conf2);
        jobConf2.setNumReduceTasks(9); 

        Job job2 = Job.getInstance(jobConf2, "phase2");
        job2.setJarByClass(Similar.class);

        job2.setMapperClass(Phase2Mapper.class);
        job2.setReducerClass(Phase2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);


        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

    }
}

