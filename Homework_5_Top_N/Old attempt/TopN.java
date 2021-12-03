import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {
	
	
	public static class TopMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	    private static List<String> stopList = Arrays.asList("one", "said", "thou", "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now");
	    private static Map<String, Integer> map = new HashMap<String, Integer>();
	    private static PriorityQueue<Entry<String, Integer>> pq;
	    
	    @Override
	    public void setup(Context context) throws IOException, InterruptedException {
	    	pq = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
	    		@Override
	    		public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
	    			if(a.getValue() == b.getValue()) {
	    				return a.getKey().compareTo(b.getKey());
	    			} else {
	    				return a.getValue() - b.getValue();
	    			}
	    		}
	    	});
	    }
	    
	    @Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
	    	
	    	StringTokenizer tokenizer = new StringTokenizer(line);
	    	
	    	while(tokenizer.hasMoreTokens()) {
	    		String token = tokenizer.nextToken();
	    		if(stopList.contains(token)) {
	    			continue;
	    		}
	    		if(!map.containsKey(token)) {
	    			map.put(token, 1);
	    		} else {
	    			Integer temp = map.get(token) + 1;
	    			map.replace(token, temp);
	    		}
	    	}
	    	
	    	Iterator<Map.Entry<String, Integer>> mapIterator = map.entrySet().iterator();
	    	
	    	while(mapIterator.hasNext()) {
	    		pq.add(mapIterator.next());
	    		if(pq.size() > 5) {
	    			pq.remove();
	    		}
	    	}
	    }
	    	
	    @Override
	    public void cleanup(Context context) throws IOException, InterruptedException {
	    	while(!pq.isEmpty()) {
	    		Map.Entry<String, Integer> entry = pq.remove();
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
	    	}
		}
	}

  public static class TopReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
	  private static PriorityQueue<Map.Entry<String, Integer>> pq;
    
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException {
		  pq = new PriorityQueue<Map.Entry<String, Integer>>(new Comparator<Map.Entry<String, Integer>>(){
	    		@Override
	    		public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
	    			if(a.getValue() == b.getValue()) {
	    				return a.getKey().compareTo(b.getKey());
	    			} else {
	    				return a.getValue() - b.getValue();
	    			}
	    		}
	    	});
	  }
	  
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		  Integer sum = 0;
		  for(IntWritable val: values) {
			  sum += val.get();
		  }
		 
		  Map.Entry<String, Integer> entry = new AbstractMap.SimpleEntry<>(key.toString(), sum);
		  pq.add(entry);
		  if(pq.size() > 5) {
			  pq.remove();
		  }	  
	  }
	  
	  @Override
	  public void cleanup(Context context) throws IOException, InterruptedException {
		  while(!pq.isEmpty()) {
	    		Map.Entry<String, Integer> entry = pq.remove();
	    		context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		  }
	  }
  }

  public static void main(String[] args) throws Exception {
	  if(args.length != 2) {
			 System.err.println("Usage: Top N <input path> <output path>");
			 System.exit(0);
		}
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Top N");
	    job.setJarByClass(TopN.class);
	    job.setMapperClass(TopMapper.class);
	    job.setReducerClass(TopReducer.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}