package hit.lqr.service.query;

import hit.lqr.dao.QueryDao;
import hit.lqr.model.XMLNode;
import hit.lqr.translation.QueryTranslation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author LiuQiuru
 * 基于MR的XML文档查询算法
 *
 */
public class QueryWithMR {
	
	private static ArrayList<String> translationSet = new ArrayList<String>();
	private static ArrayList<XMLNode> commonElement = new ArrayList<XMLNode>();
	private static String tablename = "xmlTable";
	private static QueryDao qdao = new QueryDao();

	public ArrayList<String> getTranslationSet() {
		return translationSet;
	}

	public void setTranslationSet(ArrayList<String> translationSet) {
			QueryWithMR.translationSet = translationSet;
	} 
	
	public QueryWithMR(ArrayList<String> translationSet) {
		this.translationSet = translationSet;
	}
	
	public QueryWithMR() {	
	}
	
	public static class QueryMapper extends TableMapper<Text,Text> {
		private static ArrayList<String> commonNodeSet = new ArrayList<String>();
		private static ArrayList<String> queryNodeSet = new ArrayList<String>();
		private static ArrayList<String> nodeSets = new ArrayList<String>();
		private String file1 = "hdfs://node101:9006/user/Vincent/node/commonNodeSet.txt";
		private String file2 = "hdfs://node101:9006/user/Vincent/node/queryNodeSet.txt";
		private String file3 = "hdfs://node101:9006/user/Vincent/node/nodeSets.txt";
		/*private static FSDataOutputStream out1 = null;
		private static FSDataOutputStream out2 = null;
		private static FSDataOutputStream out3 = null;*/
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String queryStatement = conf.get("queryStatement");
			QueryTranslation qt = new QueryTranslation();
			translationSet = qt.translate(queryStatement);
			//FileSystem fs = FileSystem.get(conf);
			/*out1 = fs.create(new Path(file1));
			out2 = fs.create(new Path(file2));
			out3 = fs.create(new Path(file3));*/
		}

		@Override
		public void map(ImmutableBytesWritable key, Result values, Context context) throws IOException, InterruptedException {
			//byte[] nextLine = "\n".getBytes();
			//先将该行所对应的节点信息保存在节点集合中
			//XMLNode xNode = new XMLNode();
			String cols = null;
			/*if(qdao.getData(tablename, Bytes.toString(key.get()), "element", "value") != null) {
				cols = "element";
				//元素节点
				xNode.setJudge(1);
			} else {
				cols = "attribute";
				//属性节点
				xNode.setJudge(0);
			}
	        xNode.setDocName(qdao.getData(tablename, Bytes.toString(key.get()), cols, "docName"));
			xNode.setElementName(qdao.getData(tablename, Bytes.toString(key.get()), cols, "name"));
			xNode.setPathId(Integer.parseInt(qdao.getData(tablename, Bytes.toString(key.get()), cols, "pathID")));
			xNode.setStart(Integer.parseInt(qdao.getData(tablename, Bytes.toString(key.get()), cols, "start")));
	        xNode.setEnd(Integer.parseInt(qdao.getData(tablename, Bytes.toString(key.get()), cols, "end")));
	        xNode.setLevel(Integer.parseInt(qdao.getData(tablename, Bytes.toString(key.get()), cols, "level")));
	        xNode.setValue(qdao.getData(tablename, Bytes.toString(key.get()), cols, "value"));*/
	        //add
	        /*Configuration conf = context.getConfiguration();
	        BytesWritable BytesNodeSets = DefaultStringifier.load(conf, "nodeSets", BytesWritable.class);
	        nodeSets = (ArrayList<XMLNode>)transferMRC(BytesNodeSets.getBytes());
	        System.out.println("nodeSet before:" + nodeSets);
	        nodeSets.add(xNode);
	        System.out.println("nodeSet after:" + nodeSets);*/
	        
	        //newadd
	        int nodeFlag = 0;
	        if(values.getValue(Bytes.toBytes("element"), "value".getBytes()) != null) {
				cols = "element";
				//元素节点
				nodeFlag = 1;
			} else {
				cols = "attribute";
				//属性节点
				nodeFlag = 0;
			}
	        String docName = Bytes.toString(values.getValue(cols.getBytes(), "docName".getBytes()));
	        String elementName = Bytes.toString(values.getValue(cols.getBytes(), "name".getBytes()));
	        String pathId = Bytes.toString(values.getValue(cols.getBytes(), "pathID".getBytes()));
	        String start = Bytes.toString(values.getValue(cols.getBytes(), "start".getBytes()));
	        String end = Bytes.toString(values.getValue(cols.getBytes(), "end".getBytes()));
	        String level = Bytes.toString(values.getValue(cols.getBytes(), "level".getBytes()));
	        String value = Bytes.toString(values.getValue(cols.getBytes(), "value".getBytes()));
	        
	        String nodeStr = docName + "&" + elementName + "&" + pathId + "&" + start + "&" + end + "&" + level + "&" + value + "&" + nodeFlag;
			nodeSets.add(nodeStr);
	        /*out3.write(nodeStr.getBytes(), 0, nodeStr.getBytes().length);
			out3.write(nextLine,0,nextLine.length); */
			
			Text newkey = new Text("xmlquery");
			String pathExp =null;
			for(KeyValue kv : values.list()) {
				if("path".equals(Bytes.toString(kv.getFamily())) && "name".equals(Bytes.toString(kv.getQualifier()))) {
					pathExp = Bytes.toString(kv.getValue());
					//System.out.println(pathExp);
				}
			}
			
			//for(String record : translationSet) {
			for(int i=0;i<translationSet.size();i++) {
				String record = translationSet.get(i);
				int len = record.length();
				if(len == 0) {
					continue;
				}
				
				char flag = record.charAt(0);
				String conditionStr;        //条件语句
				String commonStr;          //公共节点或公共“链
				String conditionFlag;
				String[] splitStr;
				String symbol = "";
				String conNodeKey = null;
				
				//当语句为无用语句时，跳过
				if(flag == '0') {                                                                                                              
					continue;
				}
				
				//当语句为条件语句时
				if(flag == '1') {                                                                                                               
					splitStr = record.split("&");
					conditionFlag = splitStr[1];
					conditionStr = splitStr[2];
					commonStr = splitStr[3];
					//System.out.println("公共语句为：" + conditionFlag + "&" + commonStr);
					
					 //提取条件语句中的比较符号
					for(int j=0;j<conditionStr.length();j++) {                                                         
						if(conditionStr.charAt(j) == '>' || conditionStr.charAt(j) == '<' || conditionStr.charAt(j) == '=' || conditionStr.charAt(j) == '!') {
						   symbol = symbol + conditionStr.charAt(j);
						}
					}
					
					String[] splitPre = conditionStr.split("[><=!]");
					//System.out.println(isMatch(splitPre[0],pathExp));
					String vals = null;
					/*for(KeyValue kv : values.list()) {
						if("path".equals(Bytes.toString(kv.getFamily())) && "name".equals(Bytes.toString(kv.getQualifier()))) {
							String pathExp = Bytes.toString(kv.getValueArray());*/
							//如果条件语句与该行路径相匹配，获得该行rowkey所对应的值（可能为元素节点或属性节点）
					if(isMatch(splitPre[0],pathExp)) {
						String eleVals = qdao.getData(tablename, Bytes.toString(key.get()), "element", "value");
						String attVals = qdao.getData(tablename, Bytes.toString(key.get()), "attribute", "value");
						if(eleVals !=null) {
							vals = eleVals;
						}else {
							vals = attVals;
						}
						
						if(symbol.equals(">")) {
    						if(Integer.parseInt(vals) > Integer.parseInt(splitPre[1])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    							
    						}
    					}
    					
    					if(symbol.equals(">=")) {
    						if(Integer.parseInt(vals) >= Integer.parseInt(splitPre[2])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    						}
    					}
    					
    					if(symbol.equals("<")) {
    						if(Integer.parseInt(vals) < Integer.parseInt(splitPre[1])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    						}
    					}
    					
    					if(symbol.equals("<=")) {
    						if(Integer.parseInt(vals) <= Integer.parseInt(splitPre[2])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    						}
    					}
    					
    					if(symbol.equals("!=")) {
    						if(Integer.parseInt(vals) != Integer.parseInt(splitPre[2])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    						}
    					}
    					
    					if(symbol.equals("=")) {
    						vals = "'" + vals + "'";
    						if(vals.equals(splitPre[1])) {
    							String newValue = conditionFlag + "&" + Bytes.toString(key.get());
    							context.write(newkey, new Text(newValue));
    						}
    					}
					}
					
					//接下来是处理公共链，若公共链以单“/”开始，先让commonStr = "/" + commonStr,便于用匹配
					if(commonStr.charAt(0) == '/' && commonStr.charAt(1) != '/') {
						commonStr = "/" + commonStr;
					}
					//test
					if(pathExp.equals("/entry")) {
						System.out.println(commonStr + "***" + pathExp + "***" + isMatch(commonStr,pathExp));
					}
					//如果公共语句和该行路径匹配,将条件标识和公共节点对应的行键一起存储
					if(isMatch(commonStr,pathExp)) {
						//commonNodeSet.add(conditionFlag + "&" + Bytes.toString(key.get()));
						//System.out.println("map公共节点集合：" + commonNodeSet);
						
						//newadd
						String str = conditionFlag + "&" + Bytes.toString(key.get());
						System.out.println("map公共节点信息：" + str);
						commonNodeSet.add(str);
						/*out1.write(str.getBytes(), 0, str.getBytes().length);
						out1.write(nextLine,0,nextLine.length);*/
					}
						//}
						
					//}
					
				}
				
				//得到实际查询语句
				if(flag == '2') {
					splitStr = record.split("&");
					//实际查询语句
					String queryStr = splitStr[1];
					//若该行对应的路径与实际查询语句相匹配，将该行的行键存入到queryNodeSet中
					if(isMatch(queryStr,pathExp)) {
						//add
						//BytesWritable BytesQueryNodeSet = DefaultStringifier.load(conf, "queryNodeSet", BytesWritable.class);
						//queryNodeSet = (ArrayList<String>)transferMRC(BytesQueryNodeSet.getBytes());
						
						//queryNodeSet.add(Bytes.toString(key.get()));
						//System.out.println("map查询节点集合：" + queryNodeSet);
						
						//newadd
						String str = Bytes.toString(key.get());
						System.out.println("map查询节点信息：" + str);
						queryNodeSet.add(str);
						System.out.println("map :" + queryNodeSet);
						/*out2.write(str.getBytes(), 0, str.getBytes().length);
						out2.write(nextLine,0,nextLine.length);*/
					}
				}
 			}
			
		}

		//每个节点的所有map执行完都会执cleanup函数，即若有3个节点执行，则会执行3次cleanup
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream out1 = null;
			FSDataOutputStream out2 = null;
			FSDataOutputStream out3 = null;
			//每次执行完后向文件中追加，而不要重新建文件
			if(!fs.exists(new Path(file1))) {
				out1 = fs.create(new Path(file1));
			} else {
				out1 = fs.append(new Path(file1));
			}
			if(!fs.exists(new Path(file2))) {
				out2 = fs.create(new Path(file2));
			} else {
				out2 = fs.append(new Path(file2));
			}
			if(!fs.exists(new Path(file3))) {
				out3 = fs.create(new Path(file3));
			} else {
				out3 = fs.append(new Path(file3));
			}
			
			/*FSDataOutputStream out1 = fs.create(new Path(file1));
			FSDataOutputStream out2 = fs.create(new Path(file2));
			FSDataOutputStream out3 = fs.create(new Path(file3));*/
			System.out.println("cleanup :" + queryNodeSet);
			byte[] nextLine = "\n".getBytes();
			//分别将公共节点信息、查询节点信息及所有节点信息写入HDFS中
			for(String str : commonNodeSet) {
				out1.write(str.getBytes(), 0, str.getBytes().length);
				out1.write(nextLine,0,nextLine.length);
			}
			for(String str : queryNodeSet) {
				out2.write(str.getBytes(), 0, str.getBytes().length);
				out2.write(nextLine,0,nextLine.length);
			}
			/*for(int i=0;i<3;i++) {
				out2.write("hello".getBytes(), 0, "hello".getBytes().length);
				out2.write(nextLine,0,nextLine.length);
			}*/
			for(String str : nodeSets) {
				out3.write(str.getBytes(), 0, str.getBytes().length);
				out3.write(nextLine,0,nextLine.length);
			}
			
			out1.close();
			out2.close();
			out3.close();
		}	
		
		
	}
		
	public static class QueryReducer extends Reducer<Text,Text,Text,Text> {
		private static ArrayList<XMLNode> nodeSet = new ArrayList<XMLNode>();
		private static ArrayList<String> commonNodeSet = new ArrayList<String>();
		private static ArrayList<String> queryNodeSet = new ArrayList<String>();
		private String file1 = "hdfs://node101:9006/user/Vincent/node/commonNodeSet.txt";
		private String file2 = "hdfs://node101:9006/user/Vincent/node/queryNodeSet.txt";
		private String file3 = "hdfs://node101:9006/user/Vincent/node/nodeSets.txt";
		/*private static FileSystem fs = null;
		private static FSDataInputStream in1 = null;
		private static FSDataInputStream in2 = null;
		private static FSDataInputStream in3 = null;
		private static BufferedReader bd1 = null;
		private static BufferedReader bd2 = null;
		private static BufferedReader bd3 = null;*/
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in1 = fs.open(new Path(file1));
			FSDataInputStream in2 = fs.open(new Path(file2));
			FSDataInputStream in3 = fs.open(new Path(file3));
			BufferedReader bd1 = new BufferedReader(new InputStreamReader(in1));
			BufferedReader bd2 = new BufferedReader(new InputStreamReader(in2));
			BufferedReader bd3 = new BufferedReader(new InputStreamReader(in3));
			
			String nodeLine = null;
			while(null != (nodeLine = bd3.readLine())) {
				String[] splitLine = nodeLine.split("&");
				XMLNode node = new XMLNode();
			    node.setDocName(splitLine[0]);
			    node.setElementName(splitLine[1]);
			    node.setPathId(Integer.parseInt(splitLine[2]));
			    node.setStart(Integer.parseInt(splitLine[3]));
			    node.setEnd(Integer.parseInt(splitLine[4]));
			    node.setLevel(Integer.parseInt(splitLine[5]));
			    
			    node.setValue(splitLine[6]);
			    node.setJudge(Integer.parseInt(splitLine[7]));
			    nodeSet.add(node);
			}
			
			String commonNodeLine = null;
			while(null != (commonNodeLine = bd1.readLine())) {
				commonNodeSet.add(commonNodeLine);
			}
			
			String queryKey = null;
			while(null != (queryKey = bd2.readLine())) {
				queryNodeSet.add(queryKey);
			}
			
			bd1.close();
			bd2.close();
			bd3.close();
			//fs.close();   //不能关闭fs，否则整个HDFS文件系统都关闭，无法向HDFS中写
			in1.close();
			in2.close();
			in3.close();
		}



		public void reduce(Text key,Iterable<Text> values,Context context)  throws IOException,InterruptedException {
			Iterator<Text> ite =  values.iterator();
			//存储调条件节点信息
			XMLNode conNode = new XMLNode();
			//String commonNodeLine = null;
			while(ite.hasNext()) {
				String cval = ite.next().toString();
				System.out.println(cval);
				String[] splitVal = cval.split("&");
				String cflag1 = splitVal[0];
				String conNodeKey = splitVal[1];
				
				//while(null != (commonNodeLine = bd1.readLine())) {
				
				for(String e : commonNodeSet) {
					String[] splitV = e.split("&");
					//String[] splitV = commonNodeLine.split("&");
					String cflag2 = splitV[0];
					String commonNodeKey = splitV[1];
					if(cflag1.equals(cflag2)) {
						//得到条件节点的详细信息
						//首先判断是元素节点还是条件节点
						String colFs = null;
						if(qdao.getData(tablename, conNodeKey, "element", "value") != null) {
							colFs = "element";
							conNode.setJudge(1);
						} else {
							colFs = "attribute";
							conNode.setJudge(0);
						}
						conNode.setDocName(qdao.getData(tablename, conNodeKey, colFs, "docName"));
						conNode.setElementName(qdao.getData(tablename, conNodeKey, colFs, "name"));
						conNode.setPathId(Integer.parseInt(qdao.getData(tablename, conNodeKey, colFs, "pathID")));
						conNode.setStart(Integer.parseInt(qdao.getData(tablename, conNodeKey, colFs, "start")));
				        conNode.setEnd(Integer.parseInt(qdao.getData(tablename, conNodeKey, colFs, "end")));
				        conNode.setLevel(Integer.parseInt(qdao.getData(tablename, conNodeKey, colFs, "level")));
				        conNode.setValue(qdao.getData(tablename, conNodeKey, colFs, "value"));
				        System.out.println(conNode);
				        XMLNode commonNode = new XMLNode();
				        //获得公共节点的信息
				        if(qdao.getData(tablename, commonNodeKey, "element", "value") != null) {
							colFs = "element";
							commonNode.setJudge(1);
						} else {
							colFs = "attribute";
							commonNode.setJudge(0);
						}
				        
				        commonNode.setDocName(qdao.getData(tablename, commonNodeKey, colFs, "docName"));
				        commonNode.setElementName(qdao.getData(tablename, commonNodeKey, colFs, "name"));
				        commonNode.setPathId(Integer.parseInt(qdao.getData(tablename, commonNodeKey, colFs, "pathID")));
				        commonNode.setStart(Integer.parseInt(qdao.getData(tablename, commonNodeKey, colFs, "start")));
				        commonNode.setEnd(Integer.parseInt(qdao.getData(tablename, commonNodeKey, colFs, "end")));
				        commonNode.setLevel(Integer.parseInt(qdao.getData(tablename, commonNodeKey, colFs, "level")));
				        commonNode.setValue(qdao.getData(tablename, commonNodeKey, colFs, "value"));
				        
				        //根据上面条件语句得到的节点（子节点）来判定公共链上的最后一个公共节点（祖先节点）
				        if(commonNode.getDocName().equals(conNode.getDocName()) && (commonNode.getStart() < conNode.getStart()) && (commonNode.getEnd() > conNode.getEnd()) && (commonNode.getLevel() < conNode.getLevel())) {
				        	//add
							//BytesWritable BytesCommonElement = DefaultStringifier.load(conf, "commonElement", BytesWritable.class);
							//commonElement = (ArrayList<XMLNode>)transferMRC(BytesCommonElement.getBytes());
							
				        	commonElement.add(commonNode);
				        	System.out.println("公共链上的最后公共节点集合:" + commonElement);
				        	//add
				        	//DefaultStringifier.store(conf, transfer(commonElement), "commonElement");
				        }
					
						
					}
				}	
			}
			
			//确定所有符合条件的公共节点的最后一个公共节点
			//add
			//BytesWritable BytesCommonElement = DefaultStringifier.load(conf, "commonElement", BytesWritable.class);
			//commonElement = (ArrayList<XMLNode>)transferMRC(BytesCommonElement.getBytes());
			//System.out.println("公共元素集合：" + commonElement);
			
			/*XMLNode lastCommonNode = new XMLNode();
			lastCommonNode = commonElement.get(0);
			for(XMLNode node : commonElement) {
				if(node.getDocName().equals(lastCommonNode.getDocName()) && (node.getStart() > lastCommonNode.getStart()) && (node.getEnd() < lastCommonNode.getEnd()) && (node.getLevel() > lastCommonNode.getLevel())) {
					lastCommonNode = node;
				}
			}
			//查询节点（子节点）与最后一个公共节点（祖先节点）进行祖先后代关系比较，根据最后一个公共节点的信息确定唯一的条件节点。
			//（注意：可能条件节点和最后一个公共节点是同一个节点。eg. //book[name='Lee']）
			
			//add
			BytesWritable BytesQueryNodeSet = DefaultStringifier.load(conf, "queryNodeSet", BytesWritable.class);
			queryNodeSet = (ArrayList<String>)transferMRC(BytesQueryNodeSet.getBytes());
			BytesWritable BytesNodeSets = DefaultStringifier.load(conf, "nodeSets", BytesWritable.class);
			nodeSets = (ArrayList<XMLNode>)transferMRC(BytesNodeSets.getBytes());
			System.out.println("reduce查询节点集合：" + queryNodeSet);
			System.out.println("reduce nodeSets:" + nodeSets);
			
			String queryKey = null;
			while(null != (queryKey = bd2.readLine())) {
				
			//for(String queryKey : queryNodeSet) {
				XMLNode queryNode = new XMLNode();
				String colFs = null;
				if(qdao.getData(tablename, queryKey, "element", "value") != null) {
					colFs = "element";
					queryNode.setJudge(1);
				} else {
					colFs = "attribute";
					queryNode.setJudge(0);
				}
				
				queryNode.setDocName(qdao.getData(tablename, queryKey, colFs, "docName"));
				queryNode.setElementName(qdao.getData(tablename, queryKey, colFs, "name"));
				queryNode.setPathId(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "pathID")));
				queryNode.setStart(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "start")));
				queryNode.setEnd(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "end")));
				queryNode.setLevel(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "level")));
				queryNode.setValue(qdao.getData(tablename, queryKey, colFs, "value"));
				
				if(queryNode.getDocName().equals(lastCommonNode.getDocName()) && (queryNode.getStart() >= lastCommonNode.getStart()) && (queryNode.getEnd() <= lastCommonNode.getEnd()) && (queryNode.getLevel() >= lastCommonNode.getLevel())) {
					//如果是属性节点，直接输出;否则，循环打印出该元素节点下的所有节点
					if(queryNode.getJudge() == 0) {
						System.out.println("属性节点：" + queryNode.getElementName() + ":" + queryNode.getValue());
						context.write(key, new Text("属性节点：" + queryNode.getElementName() + ":" + queryNode.getValue()));
					} else {
						String text = printNodeInfo(queryNode, 1);
						System.out.println(text);
						context.write(key, new Text(text));
					}
					
				}
			}*/
		}



		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Text key = new Text("queryResult");
			XMLNode lastCommonNode = new XMLNode();
			lastCommonNode = commonElement.get(0);
			for(XMLNode node : commonElement) {
				if(node.getDocName().equals(lastCommonNode.getDocName()) && (node.getStart() > lastCommonNode.getStart()) && (node.getEnd() < lastCommonNode.getEnd()) && (node.getLevel() > lastCommonNode.getLevel())) {
					lastCommonNode = node;
				}
			}
			//查询节点（子节点）与最后一个公共节点（祖先节点）进行祖先后代关系比较，根据最后一个公共节点的信息确定唯一的条件节点。
			//（注意：可能条件节点和最后一个公共节点是同一个节点。eg. //book[name='Lee']）
			/*String queryKey = null;
			while(null != (queryKey = bd2.readLine())) {*/
				
			for(String queryKey : queryNodeSet) {
				XMLNode queryNode = new XMLNode();
				String colFs = null;
				if(qdao.getData(tablename, queryKey, "element", "value") != null) {
					colFs = "element";
					queryNode.setJudge(1);
				} else {
					colFs = "attribute";
					queryNode.setJudge(0);
				}
				
				queryNode.setDocName(qdao.getData(tablename, queryKey, colFs, "docName"));
				queryNode.setElementName(qdao.getData(tablename, queryKey, colFs, "name"));
				queryNode.setPathId(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "pathID")));
				queryNode.setStart(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "start")));
				queryNode.setEnd(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "end")));
				queryNode.setLevel(Integer.parseInt(qdao.getData(tablename, queryKey, colFs, "level")));
				queryNode.setValue(qdao.getData(tablename, queryKey, colFs, "value"));
				
				if(queryNode.getDocName().equals(lastCommonNode.getDocName()) && (queryNode.getStart() >= lastCommonNode.getStart()) && (queryNode.getEnd() <= lastCommonNode.getEnd()) && (queryNode.getLevel() >= lastCommonNode.getLevel())) {
					//如果是属性节点，直接输出;否则，循环打印出该元素节点下的所有节点
					if(queryNode.getJudge() == 0) {
						System.out.println("属性节点：" + queryNode.getElementName() + ":" + queryNode.getValue());
						context.write(key, new Text("属性节点：" + queryNode.getElementName() + ":" + queryNode.getValue()));
					} else {
						String text = printNodeInfo(queryNode, 1, nodeSet);
						System.out.println(text);
						context.write(key, new Text(text));
					}
					
				}
			}
			
			/*bd1.close();
			bd2.close();
			bd3.close();
			fs.close();
			in1.close();
			in2.close();
			in3.close();*/		
		}
		

	}

	public void xmlQuery()  {
		long startTime = System.currentTimeMillis();
		//String queryStatement = "//teachers[teacher/@id='001']/teacher[name='XueNan']//summary";
		
		//test
		//String queryStatement = "//entry[gene//@type='ORF']/protein//fullName";
		//String queryStatement = "//entry[organismHost/name='Ambystoma']/reference//citation[dbReference/@id='15165820']/authorList";
		//String queryStatement = "//reference/citation[authorList/person/@name='Tan W.G.']/dbReference[@type='DOI']/@id";
		//String queryStatement = "//entry[accession='Q6GZX4']/organism[name/@type='common']//lineage[taxon='Viruses']";
		String queryStatement = "//feature[@type='chain']";
		/*QueryTranslation qt = new QueryTranslation();
		translationSet = qt.translate(queryStatement);*/
		
		System.out.println("**************************************");
	
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.rootdir","hdfs://localhost:9000/hbase");
		conf.set("fs.defaultFS", "hdfs://node101:9006/");
        conf.set("hbase.zookeeper.quorum", "node2,node3,node4");
        conf.set("hbase.rootdir","hdfs://node101:9006/hbase");
        
        //将需要向集群中传递的参数通过configuration的set函数传入，并在需要的地方通过get函数获得
        conf.set("queryStatement", queryStatement);
		
		try {
			//向集群中传入参数（对象类型）
			/*DefaultStringifier.store(conf, transfer(commonNodeSet), "commonNodeSet");
			DefaultStringifier.store(conf, transfer(queryNodeSet), "queryNodeSet");
			DefaultStringifier.store(conf, transfer(commonElement), "commonElement");
			DefaultStringifier.store(conf, transfer(nodeSets), "nodeSets");*/
			
		    String[] IOArgs = new String[] {"input", "output"};
		    String[] otherArgs = new GenericOptionsParser(conf,IOArgs).getRemainingArgs();
			if(otherArgs.length != 2) {
				System.err.println("Usage: QueryWithMR  <in> <out>"); 
				System.exit(2);
			}
			
			Job job = Job.getInstance(conf,"QueryWithMR");
			job.setJarByClass( QueryWithMR.class);
			Scan sc = new Scan();
			
			TableMapReduceUtil.initTableMapperJob(tablename, sc, QueryMapper.class, Text.class, Text.class, job);
			job.setReducerClass(QueryReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			/*endTime = System.currentTimeMillis();
	        System.out.println("程序运行的时间是：" +  (endTime - startTime) + "ms");
			
			System.exit(job.waitForCompletion(true) ? 0 :1);*/
			
			if(job.waitForCompletion(true)) {
				long endTime = System.currentTimeMillis();
				/*System.out.println("公共：" + commonNodeSet);
				System.out.println("查询：" + queryNodeSet);
				System.out.println("查询：" + nodeSets.size());*/
			    System.out.println("程序运行的时间是：" +  (endTime - startTime) + "ms");
			    System.exit(0);
			}else {
				System.exit(1);
			}
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
	
	public static boolean isMatch(String line, String pathExp) {
		//将查询语句按“/”拆分为字符串数组，便于与在hbase中存储的路径信息进行比较，从而拆分成简单路径
		String[] splitVals = line.split("/");   
		String[] splitKeys = pathExp.split("/");;
		boolean f = false;                                                                              //用来标记查询语句与路径不匹配，默认为false
		boolean flag;
		 
	    int i = 1;
		int j = 1;
		while(i<splitVals.length) {
		//while(i<splitVals.length||j<splitKeys.length) {                                               //同时从头开始遍历两个拆分后的数组，对数组的每个单元都进行比较
			if(j<splitKeys.length && (!splitVals[i].equals(""))&& (splitVals[i].equals(splitKeys[j]))) {                //若查询语句拆分的数组的某个单元i不为空串，并且与路径拆分的数组的对应单元j相等，则两个数组指针同时向后移动一格，继续比较，直到不相等或第一个数组出现空串                      
				i++;
				j++;
			}else if(j<splitKeys.length && (!splitVals[i].equals("")) && (!splitVals.equals(splitKeys[j]))) {    //若查询语句数组的某个单元i不为空串，且不与对应的路径拆分数组的对应单元j相等，则该路径不能与该查询语句相匹配，置f为true,并退出循环
				f = true;
				break;
			}else {                                                                                                                        //若查询语句拆分数组的某个单元i为空串，则说明该单元对应原查询语句的“//”，将指针向后移动1格。同时从路径拆分数组的当前位置开始扫描数组
				flag = false;                                                                                                          //如果路径拆分数组中存在某一单元与查询数组空串后的单元相等，则置flag为true，将查询语句拆分数组指针向后移动一格，并跳出循环
				i++;
				for(;j<splitKeys.length;j++) {
					if((splitVals[i].equals(splitKeys[j]))){
						i++;
						flag = true;
						break;
					}
				
				
			    }
			   
				
			    if(!flag) {                                                                                                                  //若路径拆分数组后面的单元没有与查询拆分数组空串后的单元相等的，则该路径不能于该查询语句匹配，置f为true,并跳出循环
					f = true;
					break;
				}else {                                                                                                                    //若有相等的，将路径拆分数组指针向后移动一格                                                                                          
					j++;
				}
			}
		}
		
		if(i<splitVals.length || j<splitKeys.length) {                                                  //如果存在一个数组没有遍历到结尾，说明该查询语句与此路径不匹配，置f为true
			f = true;
		}
		
		if(!f) {
			return true;
		}
	
		return false;
	}

	/**
	 * 打印节点信息（包含其下的子节点）
	 * @param context 
	 */
    private static String printNodeInfo(XMLNode queryNode, int printLevel, ArrayList<XMLNode> nodeSet) {
		String resultText = "";
		String printStr = "";
		int tmpLevel;
		
		for(int p =0;p<printLevel;p++) {                                                                                                                        //缩进，用于显示出节点之间的层次关系
    		printStr = printStr + "   ";
    	}
		
		String textValue = (queryNode.getValue()!=null) ? queryNode.getValue() : " ";
		//String textValue = " ";
		/*if(queryNode.getValue() != null) {
			textValue = queryNode.getValue();
		}*/
		System.out.println(textValue);
		resultText += (printStr + queryNode.getElementName() + ": " + textValue);
		//换行
		//resultText += "\n";
		
		//输出该元素节点的属性信息
		for(XMLNode node : nodeSet) {
			if(node.getJudge() == 0 && node.getDocName().equals(queryNode.getDocName()) && (node.getStart() == queryNode.getStart() + 1) && (node.getLevel() == queryNode.getLevel() + 1)) {
				resultText += (" " + "@" + node.getElementName() + "=" + node.getValue());
				//resultText += "\n";
			}
		}
		
		resultText += "\n";
		//循环遍历子元素节点
		for(XMLNode node : nodeSet) {
			tmpLevel = printLevel;
			if(node.getJudge() == 1 && node.getDocName().equals(queryNode.getDocName()) && node.getStart() > queryNode.getStart() && node.getEnd() < queryNode.getEnd() && (node.getLevel() == queryNode.getLevel() + 1)) {
				printLevel ++;
				resultText += printNodeInfo(node,printLevel, nodeSet);
				printLevel = tmpLevel;
			}
		}
		
		return resultText;
		
	}
    
    private static BytesWritable transfer(Object patterns) {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(patterns);
            oos.flush( );
            return new BytesWritable(baos.toByteArray( ));
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
        	try {
        		baos.close();
        		oos.close();
        	} catch(IOException e) {
        		e.printStackTrace();
        	}
        }
        return null;
    }
    
    private static Object transferMRC( byte[] bytes ) {
        //        MapWritable map = new MapWritable( );
        ObjectInputStream is = null;
        try {
            is = new ObjectInputStream( new ByteArrayInputStream( bytes ) );
            return is.readObject( );
        } catch( Exception e ) {
        	e.printStackTrace();
        } finally {
           try {
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        }
        return null;
    }
}