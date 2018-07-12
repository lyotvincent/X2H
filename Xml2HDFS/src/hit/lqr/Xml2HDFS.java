package hit.lqr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author LiuQiuru
 * 将XML文档解析后存储到HDFS中
 *
 */
public class Xml2HDFS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		String filename1 = "hdfs://node101:9006/user/Vincent/element.txt";             //要写入的hdfs文件路径
		String filename2 = "hdfs://node101:9006/user/Vincent/attribute.txt";
		String filename3 = "hdfs://node101:9006/user/Vincent/path.txt";
		String timeFile = "hdfs://node101:9006/user/Vincent/time.txt";
		//改进后的xml节点信息存储
		NodePositionInfo positionInfo = new NodePositionInfo();
		positionInfo.info2HDFS(filename1, filename2, filename3);
		
		//将XML拆分存入HDFS中的时间写入到HDFS文件中，方便查看
		Configuration conf = new Configuration();
		FileSystem fs = null;
		FSDataOutputStream out = null;
		try {
			fs = FileSystem.get(conf);
			out = fs.create(new Path(timeFile)); 
			
			long endTime = System.currentTimeMillis();
			String str = "Xml2HDFS程序运行的时间是：" +  (endTime - startTime) + "ms";
			out.write(str.getBytes(), 0, str.getBytes().length);
		    System.out.println("Xml2HDFS程序运行的时间是：" +  (endTime - startTime) + "ms");
		   
		   out.close();
		   fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		  
		

	}

}
