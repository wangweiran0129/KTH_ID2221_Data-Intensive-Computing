package id2221.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			// begin with "I" in "Id", end with " of "-1"
			// for example:
			/*
			  Id=
			  -1 
			  Reputation=
			  1 
			  CreationDate=
			  2014-05-13T21:29:22.820 
			  DisplayName=
			  Community
			  LastAccessDate=
			  2014-05-13T21:29:22.820
			  WebsiteUrl=
			  http://meta.stackexchange.com/
			  Location=
			  on the server farm
			  AboutMe=
			  &lt;p&gt;Hi, I'm not really a person.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I'm a background process that helps keep this site clean!&lt;/p&gt;&#xA;&#xA;&lt;p&gt;I do things like&lt;/p&gt;&#xA;&#xA;&lt;ul&gt;&#xA;&lt;li&gt;Randomly poke old unanswered questions every hour so they get some attention&lt;/li&gt;&#xA;&lt;li&gt;Own community questions and answers so nobody gets unnecessary reputation from them&lt;/li&gt;&#xA;&lt;li&gt;Own downvotes on spam/evil posts that get permanently deleted&lt;/li&gt;&#xA;&lt;li&gt;Own suggested edits from anonymous users&lt;/li&gt;&#xA;&lt;li&gt;&lt;a href=&quot;http://meta.stackexchange.com/a/92006&quot;&gt;Remove abandoned questions&lt;/a&gt;&lt;/li&gt;&#xA;&lt;/ul&gt;&#xA;
			  Views=
			  0 
			  UpVotes=
			  749 
			  DownVotes=
			  221 
			  AccountId=
			  -1
			*/

			// for example:
			// key: Id=
			// value: -1
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				// delete the "="
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		// Use the TreeMap to store the processed input records
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> m = transformXmlToMap(value.toString());
		if(m==null){
			return;
		}

		String userId = m.get("Id");
		String reputation = m.get("reputation");

		// skip over the rows that do not contain user data
		if(userId==null || reputation==null){
			return;
		}

		// remove unnecessary data 
		if(repToRecordMap.size()>10){
			repToRecordMap.remove(repToRecordMap.firstKey());
		}

		repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Output our ten records to the reducers with a null key
        // The cleanup method gets called once after all key-value paris have been through the map function
        /*
		int Count = 10;
		for(Integer reputation : repToRecordMap.descendingKeySet()){
			String payload = String.format("%d %s", reputation, repToRecordMap.get(reputation));
			Text t = new Text(payload);
			context.write(NullWritable.get(), t);
			Count--;
			if(Count<0){
				break;
			}
        }
        */
        for(Text t: repToRecordMap.values()){
            context.write(NullWritable.get(), t);
        }
	}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    for(Text value : values){
			// reputation = 1
            // Id = -1
            Map<String, String> m = TopTen.transformXmlToMap(value.toString());
            String reputation = m.get("Reputation");
            String userId = m.get("Id");
			//Integer reputation = Integer.parseInt(value.toString().split(" ")[0]);
			//Text userId = new Text(value.toString().split(" ")[1]);
			//repToRecordMap.put(reputation, userId);
		}
		int Count = 10;

		// Push the results into HBase
		for(Integer reputation : repToRecordMap.descendingKeySet()){
			byte[] id = repToRecordMap.get(reputation).copyBytes();
			Put insHBase = new Put(id);				
			insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), id);
			insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("reputation"), Bytes.toBytes(reputation));
			context.write(null, insHBase);
			Count--;
			if(Count<0){
				break;
			}
		}
	}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "topten");
	    job.setJarByClass(TopTen.class);

	    job.setMapperClass(TopTenMapper.class);
	    job.setReducerClass(TopTenReducer.class);

	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //define output table
	    TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		
		System.exit(job.waitForCompletion(true)?0:1);
    }
}