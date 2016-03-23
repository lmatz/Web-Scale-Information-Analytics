import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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


public class SON_3 {
	
	static class Triplet {
		private int item1;
		private int item2;
		private int item3;
		
		public Triplet(int _item1, int _item2, int _item3) {
			this.item1 = _item1;
			this.item2 = _item2;
			this.item3 = _item3;
		}
		
		public int getItem1() {
			return this.item1;
		}
		
		public int getItem2() {
			return this.item2;
		}
		
		public int getItem3() {
			return this.item3;
		}
		
		@Override
		public String toString() {
			return item1+" "+item2+" "+item3;
		}
		
	}
    
	static class Pair {

		private int item1;
		private int item2;

		public Pair(int _item1, int _item2) {
			this.item1 = _item1;
			this.item2 = _item2;
		}
		
		public int getItem1() {
			return this.item1;
		}
		
		public int getItem2() {
			return this.item2;
		}
		
		@Override
		public String toString() {
			return item1+" "+item2;
		}
		
	}


	static class MutableInt {

		int value; // note that we start at 1 since we're counting

		public void increment () {
			++value;      
		}

		public int  get () { 
			return value; 
		}
		
		public MutableInt() {
			value = 1;
		}
		
		public MutableInt(int _val) {
			value = _val;
		}
	
	}

    // phase1's Mapper's job is:
    // 1 read a chunk of input file
    // 2 count all words
	// 3 count all pairs which contain two frequent words
	// 4 count all triplets which each pair from it is frequent
    public static class Phase1Mapper
    extends Mapper<LongWritable, Text, Text, NullWritable>{

        public static HashMap<String,Integer> wordToInt;

        public static HashMap<Integer,String> intToWord;

        public static HashMap<Integer,MutableInt> frequencyWords;

        public static HashMap<Integer,Integer> differentWords;

        public static int[] frequencyPairs;

        public static ArrayList<Integer> differentPairs;

        public static HashMap<Integer,Pair> pairMap;
        
        public static HashMap<Triplet,MutableInt> tripletMap;

        public static ArrayList<String[]> baskets;

        public static int allBaskets = 0;

        public static double frequencyRequired = 0.005;

        public static int wordIndex = 1;

        public static int sizeOfDifferentWords;

        
        @Override
        protected void setup(Context context)
                throws IOException,
                InterruptedException {
            baskets = new ArrayList<String[]>();
            wordToInt = new HashMap<String,Integer>();
            intToWord = new HashMap<Integer,String>();
            frequencyWords = new HashMap<Integer,MutableInt>();
            differentWords = new HashMap<Integer,Integer>();
        }

        @Override
        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
        	
            aPrior();
           
	

 
            for ( Map.Entry<Triplet,MutableInt> entry : tripletMap.entrySet() ) {
            	
            	Triplet key = entry.getKey();
             	MutableInt val = entry.getValue();
             	
                int first = key.getItem1();
                int second = key.getItem2();
                int third = key.getItem3();
                
                
                String[] stringsOutput = new String[3];
                stringsOutput[0] = intToWord.get(first);
                stringsOutput[1] = intToWord.get(second);
                stringsOutput[2] = intToWord.get(third);
                Arrays.sort(stringsOutput);
                
                context.write( new Text( stringsOutput[0] +" "+ stringsOutput[1] +" "+ stringsOutput[2] ), NullWritable.get() );
            }
        }

        // 
        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
            String line = value.toString();
            baskets.add(line.trim().split(" "));
        }

        public void aPrior() {
            phase1();
            phase2();
            phase3();
        }

        public void phase1() {
        	
        	allBaskets = baskets.size();
            
        	// count all words
            for(int i=0;i<baskets.size();i++) {
            	
                String[] stringArray = baskets.get(i);
                
                for (String str : stringArray) {

                    Integer index = wordToInt.get(str);
                    if ( index == null ) {
                        wordToInt.put(str, wordIndex);
                        intToWord.put(wordIndex, str);
                        index = wordIndex;
                        wordIndex++;
                    }

                    MutableInt count = frequencyWords.get(index);
                    if (count == null) {
                        frequencyWords.put(index, new MutableInt());
                    }
                    else {
                        count.increment();
                    }
                }
                
            }

            // pick out all the words which is more frequent than required
            // and put it in differentWords
            differentWords = new HashMap<Integer,Integer>();

            // get these high-frequency word
            int indexOfDifferentWords=1;
            for(int i=1;i<=wordToInt.size();i++) {

                MutableInt value = frequencyWords.get(i);
                int frequency = value.get();
                
                if ( frequency >= frequencyRequired * allBaskets ) {
                    differentWords.put(i, indexOfDifferentWords);
                    indexOfDifferentWords++;
                }
                
            }

            // let the jvm gc
            frequencyWords = null;
        }

        public void phase2() {
            pairMap = new HashMap<Integer,Pair>();

            sizeOfDifferentWords = differentWords.size();

            frequencyPairs = new int[sizeOfDifferentWords * (sizeOfDifferentWords+1) / 2 + 2];

            differentPairs = new ArrayList<Integer>();

            // count all frequency of pairs
            for(int i=0;i<baskets.size();i++) {
                String[] stringArray = baskets.get(i);
                findFrequentPairs(stringArray);
            }

            for( int i=1;i<frequencyPairs.length;i++ ) {
                int times = frequencyPairs[i];
                if ( times >= frequencyRequired * allBaskets ) {
                    differentPairs.add(i);
                }
            }
        }

        public void findFrequentPairs(String[] stringArray) {
        	
            for( int i=0 ; i<stringArray.length ; i++ ) {
                for( int j=i+1 ; j<stringArray.length ; j++ ) {

                    int item1Index = wordToInt.get(stringArray[i]);
                    int item2Index = wordToInt.get(stringArray[j]);

                    if ( differentWords.containsKey(item1Index) && differentWords.containsKey(item2Index) ) {
                    	
                        int ki = differentWords.get(item1Index);
                        int kj = differentWords.get(item2Index);

                        if ( ki < kj ){
                            frequencyPairs[ (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki ] ++;
                            
                            Pair aPair = pairMap.get(  (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki );
                            if ( aPair == null ) {
                            	pairMap.put( (ki - 1)*(sizeOfDifferentWords) - (ki - 1)*ki/2 + kj - ki , new Pair(item1Index,item2Index));
                            }
                            
                        }
                        else if ( ki > kj ) {
                        	frequencyPairs[ (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj ] ++ ;
    					    
    					    Pair aPair = pairMap.get( (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj );
    					    if ( aPair == null ) {
    					    	pairMap.put( (kj - 1)*(sizeOfDifferentWords) - (kj - 1)*kj/2 + ki - kj , new Pair(item2Index,item1Index) );
    					    }
                        } 
                    }
                    else {
                        continue;
                    }
                }
            }
        }
        
        
        public void phase3() {
       
		pairMap = null;

		frequencyWords = null;
 
		tripletMap = new HashMap<Triplet,MutableInt>();
	
        	for(int i=0;i<baskets.size();i++) {
        		String[] stringArray = baskets.get(i);
        		findFrequentTriplets(stringArray);
        	}
        	
        }
        
        public void findFrequentTriplets(String[] stringArray) {
        	
        	int ki;
        	int kj;
        	int kk;
        	int[] intArray = new int[3];
        	
        	for( int i=0 ; i<stringArray.length ; i++ ) {
                for( int j=i+1 ; j<stringArray.length ; j++ ) {
                	
                	int item1Index = wordToInt.get(stringArray[i]);
                    int item2Index = wordToInt.get(stringArray[j]);
                    
                    if ( differentWords.containsKey(item1Index) && differentWords.containsKey(item2Index) ) {
                    	
                        ki = differentWords.get(item1Index);
                        kj = differentWords.get(item2Index);
                        
                        if ( ! checkWhetherFrequentPair(ki,kj)  ) {
                        	continue;
                        }
                        
                    }
                    else {
                    	continue;
                    }
                	
                	for( int k=j+1 ; k<stringArray.length;k++) {
                		
                		int item3Index = wordToInt.get(stringArray[k]);
                		
	                    if ( differentWords.containsKey(item3Index) ) {
	                    	
	                    	kk = differentWords.get(item3Index);
	                    	
	                    	if ( ! checkWhetherFrequentPair(ki,kk) ) {
	                    		continue;
	                    	}
	                    	
	                    	if ( ! checkWhetherFrequentPair(kj,kk) ) {
	                    		continue;
	                    	}
	                    	
	                    	intArray[0]=item1Index;
	                    	intArray[1]=item2Index;
	                    	intArray[2]=item3Index;
	                    	
	                    	Arrays.sort(intArray);
	                    	
	                    	Triplet triplet = new Triplet(  intArray[0],intArray[1],intArray[2]  );
	                    	
	                    	MutableInt count = tripletMap.get( triplet );
	                
	                    	if ( count == null ) {
	                    		tripletMap.put(triplet, new MutableInt(1));
	                    	}
	                    	else {
	                    		count.increment();
	                    	}
	                    	
	                    }
	                    else {
	                        continue;
	                    }
                	}
                }
            }
        }
        
        public boolean checkWhetherFrequentPair(int first, int second) {
        	if ( first < second ){
                if (  frequencyPairs[ (first - 1)*(sizeOfDifferentWords) - (first - 1)*first/2 + second - first ] < frequencyRequired * allBaskets  ) {
                	return false;
                }
            }
            else if ( first > second ) {
			    if (  frequencyPairs[ (second - 1)*(sizeOfDifferentWords) - (second - 1)*second/2 + first - second ] < frequencyRequired * allBaskets  ) {
			    	return false;
			    }
            }
        	return true;
        }
        
        
    }

    public static class Phase1Reducer
    extends Reducer<Text,NullWritable,Text,NullWritable> {

        public void reduce(Text key, Iterable<NullWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get() );
        }
    }
    

    public static class Phase2Mapper
    extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        public static HashMap<String,MutableInt> frequencyTriplet;
        
        public static ArrayList<String[]> baskets;

	public static String supplementFile;	

	public static int allBaskets = 0;

        @Override
        protected void setup(Context context)
                throws IOException,
                InterruptedException {
        	
        	supplementFile = context.getConfiguration().get("supplementFile");
            baskets = new ArrayList<String[]>();
            frequencyTriplet = new HashMap<String,MutableInt>();
            Path pt=new Path("hdfs://HadoopMaster:9000"+supplementFile+"/part-r-00000");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            
            try {
                String line;
                line=br.readLine();
                while (line != null) {	
		            String[] triplet = line.trim().split(" ");
		            Arrays.sort(triplet);
		            
		            frequencyTriplet.put( triplet[0]+" "+triplet[1]+" "+triplet[2] , new MutableInt(0));
	                line = br.readLine();
                }
            } finally {
                br.close();
            }
        }
        

        @Override
        protected void cleanup(Context context)
                throws IOException,
                InterruptedException {
        
		context.write( new Text(" ") , new IntWritable(allBaskets) );

	
            for( Map.Entry<String,MutableInt> entry : frequencyTriplet.entrySet() ) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue().get()));
            }
            
        }
        

        @Override
        public void map(LongWritable key, Text value, Context context
                ) throws IOException, InterruptedException {
        	
            String line = value.toString();
            String[] triplets = line.trim().split(" ");
            
	    allBaskets ++;

            for(int i=0;i<triplets.length;i++) {
            	for(int j=i+1;j<triplets.length;j++) {
            		for(int k=j+1;k<triplets.length;k++) {
            		
	            		String[] stringCount = new String[3];
	            		
	            		stringCount[0] = triplets[i];
	            		stringCount[1] = triplets[j];
	            		stringCount[2] = triplets[k];
	            		
	            		Arrays.sort(stringCount);
	            		
	            		MutableInt count = frequencyTriplet.get( stringCount[0] +" "+ stringCount[1] +" "+ stringCount[2] );
	                    if ( count == null ) {
	
	                    }
	                    else {
	                        count.increment();
	                    }
            		}
            	}
            }
        }
        
        
    }

    public static class Phase2Reducer
    extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    	HashMap<String,Integer> sortMap;

	public static int allBaskets = 0;

		@Override
		protected void setup(Context context)
	              throws IOException,
	                     InterruptedException {
			sortMap = new HashMap<String,Integer>();
		}


		@Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
            int all = 0;
            for ( IntWritable val : values) {
                all += val.get();
            }

	    if ( key.toString().equals(" ") ) {
		allBaskets = all;
	    }
	    else {
            	sortMap.put( key.toString() , all );
            }
	}

		
		@Override
		protected void cleanup(Context context)
	                throws IOException,
	                       InterruptedException {
			
			    List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>( sortMap.entrySet() );
			    
			    Collections.sort( list, new Comparator<Map.Entry<String,Integer>>(){
		    		public int compare( Map.Entry<String,Integer> o1, Map.Entry<String,Integer> o2 ) {
	                    return (o1.getValue()).compareTo( o2.getValue());
	                }
			    });
			    
			    int listSize = list.size();
			    
			    int outputNumber = Math.min(20, listSize);
			    
			    for(int i = listSize-outputNumber ; i<listSize ;i++ ) {
					Map.Entry<String,Integer> entry = list.get(i);
					context.write( new Text(entry.getKey()) , new DoubleWritable(  (double)entry.getValue()/allBaskets  ) );
					
			    }
			}
	}



    public static void main(String[] args) throws Exception {

        // phase1
        Configuration conf = new Configuration();
//        conf.setBoolean("mapred.compress.map.output", true);  
//        conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);

        JobConf jobConf = new JobConf(conf);
        jobConf.setJobName("Phase1");
        jobConf.setNumReduceTasks(1);

        Job job = Job.getInstance(jobConf, "phase1");
        job.setJarByClass(SON_3.class);

        job.setMapperClass(Phase1Mapper.class);
        job.setReducerClass(Phase1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        FileOutputFormat.setCompressOutput(job, true);  
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); 

        // inputfile
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // outputfile as the supplement file for the next phase
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        

        job.waitForCompletion(true);

        // phase2
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.output.textoutputformat.separator", ":");
        conf2.setBoolean("mapred.compress.map.output", true);  
        conf2.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class); 

        JobConf jobConf2 = new JobConf(conf2);
        jobConf2.setNumReduceTasks(1); 
        jobConf2.set("supplementFile",args[1]);	


        Job job2 = Job.getInstance(jobConf2, "phase2");
        job2.setJarByClass(SON_3.class);

        job2.setMapperClass(Phase2Mapper.class);
        job2.setReducerClass(Phase2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);


        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // inputfile is the same as inputfile in phase1
        FileInputFormat.addInputPath(job2, new Path(args[0]));

        // outputfile is the result
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
    
}
