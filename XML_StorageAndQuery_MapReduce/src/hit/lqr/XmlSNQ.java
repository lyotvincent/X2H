package hit.lqr;

import hit.lqr.service.storage.File2Hbase;
import hit.lqr.service.storage.NodePosition;
import hit.lqr.service.storage.NodePositionInfo;
/**
 * 
 * @author LiuQiuru
 * 该部分将HDFS中拆分后的XML数据存储到Hbase中
 *
 */
public class XmlSNQ {
	public static void main(String[] args) {
		
		long startTime = System.currentTimeMillis();
		//伪分布环境测试
		/*String filename1 = "hdfs://localhost:9000/user/hadoop/element.txt";             //要写入的hdfs文件路径
		String filename2 = "hdfs://localhost:9000/user/hadoop/attribute.txt";
		String filename3 = "hdfs://localhost:9000/user/hadoop/path.txt";*/
		
		//分布式环境测试
		String filename1 = "hdfs://node101:9006/user/Vincent/element.txt";             //要写入的hdfs文件路径
		String filename2 = "hdfs://node101:9006/user/Vincent/attribute.txt";
		String filename3 = "hdfs://node101:9006/user/Vincent/path.txt";
		
		String tableName = "xmlTable";
		//String colFamily1 = "element";
		//String colFamily2 = "attribute";
		//String colFamily3 = "text";
		//String colFamily3 = "path";
		
		//String tablename1 = "testelement";
		//String tablename2 = "testattribute";
		//String tablename3 = "testtext";
		//String tablename4 = "testpath";
			
		
		/*NodePosition position = new NodePosition();
		position.info2HDFS(filename1, filename2, filename3);*/
		
		//改进后的xml节点信息存储
		/*NodePositionInfo positionInfo = new NodePositionInfo();
		positionInfo.info2HDFS(filename1, filename2, filename3);*/

		//File2Hbase fh1 = new File2Hbase(colFamily1);
		//File2Hbase.setColumnFamily(colFamily1);
		File2Hbase fh1 = new File2Hbase();
		fh1.file2Hbase(filename1, tableName);
		
		//File2Hbase.setColumnFamily(colFamily2);
		File2Hbase fh2 = new File2Hbase();
		fh2.file2Hbase(filename2, tableName);
		
		//File2Hbase.setColumnFamily(colFamily3);
		File2Hbase fh3 = new File2Hbase();
		fh3.file2Hbase(filename3, tableName);
		
		/*fh1.file2Hbase(filename1, tableName);
		fh2.file2Hbase(filename2, tableName);
		fh3.file2Hbase(filename3, tableName);*/
		/*File2Hbase.file2Hbase(filename1, tablename1);
		File2Hbase.file2Hbase(filename2, tablename2);
		File2Hbase.file2Hbase(filename3, tablename3);
		File2Hbase.file2Hbase(filename4, tablename4);*/
		
		long endTime = System.currentTimeMillis();
	    System.out.println("程序运行的时间是：" +  (endTime - startTime) + "ms");
		
	}

}
