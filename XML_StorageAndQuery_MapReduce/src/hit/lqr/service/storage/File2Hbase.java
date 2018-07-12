package hit.lqr.service.storage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class File2Hbase {
	//private static String columnFamily = "";
	
	/*public File2Hbase(String columnFamily) {
		File2Hbase.columnFamily = columnFamily;
	}*/
	
	public File2Hbase() {	
	}

	/*public static String getColumnFamily() {
		return columnFamily;
	}

	public static void setColumnFamily(String columnFamily) {
		File2Hbase.columnFamily = columnFamily;
	}*/

	public static class MapperClass extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	  //列名
		public static final String[] COLUMNS = { "docName","nodeID", "pathID", "name","start" ,"end","level","value"};
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] cloumnVals = value.toString().split("&");
			//String rowkey = cloumnVals[0] + cloumnVals[1] + cloumnVals[2];
			String rowkey = cloumnVals[1] + cloumnVals[2] + cloumnVals[3];
			//列族名
			String family = cloumnVals[0];
			Put put = new Put(rowkey.getBytes());
			for (int i = 1; i < cloumnVals.length; i++) {
				//put.add(("" + columnFamily).getBytes(), COLUMNS[i].getBytes(), cloumnVals[i].getBytes());
				//put.addColumn(columnFamily.getBytes(), COLUMNS[i].getBytes(), cloumnVals[i].getBytes());
				//test
				put.addColumn(Bytes.toBytes(family), Bytes.toBytes(COLUMNS[i-1]), Bytes.toBytes(cloumnVals[i]));
				//System.out.println((columnFamily).getBytes()); //有问题
				//System.out.println(COLUMNS[i].getBytes());
				//System.out.println(cloumnVals[i].getBytes());
			}
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
	  }
}

//public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	public void file2Hbase(String filename, String tablename) {
	  Configuration conf = new Configuration();
	  /*conf.set("fs.defaultFS", "hdfs://ubuntu:9000/");
	  conf.set("mapreduce.framework.name", "local");
	  conf.set("mapred.job.tracker", "ubuntu:9001");
	  conf.set("hbase.zookeeper.quorum", "ubuntu");*/
	  
	  //伪分布测试
	  /*conf.set("fs.defaultFS", "hdfs://localhost:9000/");
	  conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");*/
	  
	  //分布式环境测试
	  conf.set("fs.defaultFS", "hdfs://node101:9006/");
	  conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
	  conf.set("hbase.rootdir","hdfs://node101:9006/hbase");
	  //Job job = new Job(conf,"file2hbase");
	  try {
		  Job job = Job.getInstance(conf, "file2hbase");
		  job.setJarByClass(File2Hbase.class);
		  job.setMapperClass(MapperClass.class);
		  job.setNumReduceTasks(0);
		  job.setOutputFormatClass(TableOutputFormat.class);
		  job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
		  FileInputFormat.addInputPath(job, new Path(filename));
		  System.out.println(job.waitForCompletion(true) ? 0 : 1);
		 } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	    }catch (IOException e) {
		// TODO Auto-generated catch block
	    	e.printStackTrace();
	}
}

}

