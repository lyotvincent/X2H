package hit.lqr;

import hit.lqr.service.storage.NodePositionInfo;
/**
 * 
 * @author LiuQiuru
 * 此部分已单独提出，该工程下可以不做考虑
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
		
		//改进后的xml节点信息存储
		NodePositionInfo positionInfo = new NodePositionInfo();
		positionInfo.info2HDFS(filename1, filename2, filename3);
		
		long endTime = System.currentTimeMillis();
	    System.out.println("Xml2HDFS程序运行的时间是：" +  (endTime - startTime) + "ms");

	}

}
