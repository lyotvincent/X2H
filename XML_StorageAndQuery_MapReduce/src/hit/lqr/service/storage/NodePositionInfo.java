package hit.lqr.service.storage;

import hit.lqr.model.XMLNode;
import hit.lqr.model.PathInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;
import org.dom4j.io.SAXReader;

/**
 * 对xml的解析（NodePosition.java）进行改进，利用dom4j的sax机制，在读取每个节点时就进行处理，并将访问过的节点从内存中删去，避免消耗内存
 * 过大的问题出现,该部分已经单独提出
 * @author LiuQiuru
 *
 */
public class NodePositionInfo {
	private int nid = 1;       //记录xml数前序遍历（包括返回原节点）时访问的节点编号，用于记录节点的位置信息
	private int nodeID = 1;   //记录节点的编号信息
	private int pathID = 1;   //记录路径信息的编号
	private int elementNum = 0;  //记录文档中元素节点的个数
	private int attributeNum = 0;  //记录文档中属性节点的个数
	private ArrayList<XMLNode> eleInfo = new ArrayList<XMLNode>();
	private ArrayList<XMLNode> attrInfo = new ArrayList<XMLNode>();
	private ArrayList<PathInfo> pathInfo = new ArrayList<PathInfo>();
	private ArrayList<Element> elementSet = new ArrayList<Element>();
	
	
	public void info2HDFS(String filename1, String filename2, String filename3) {
		byte[] nextLine = "\n".getBytes();
		String str;
		//final String xmlName = "test1.xml";
        //final String xmlName = "/public2/home/Vincent/test/input/test1.xml";
		//final String xmlName = "/public2/home/Vincent/test/XMLTest/input/uniprot1.xml";
		final String xmlName = "/public2/home/Vincent/test/XMLTest/input/test1.xml";
		//final String xmlName = "/public2/home/Vincent/test/XMLTest/input/uniprot_sprot.xml";
		
		SAXReader reader = new SAXReader();
		reader.setDefaultHandler(new ElementHandler(){ 
		    public void onStart(ElementPath arg0) {
		    	//boolean f = false;
		    	XMLNode elementNode = new XMLNode();
		    	Element e = arg0.getCurrent(); //获得当前节点 
		    	elementSet.add(e);
		    	 //System.out.println(e.getPath());
		    	PathInfo pInfo = new PathInfo();
		    	pInfo.setDocName(xmlName);
		    	pInfo.setNodeID(nodeID);
		    	pInfo.setPathexp(arg0.getPath());
		    	/*for(int i=0;i<pathInfo.size();i++) {
		 		//判断当前表达式在表达式列表中是否出现过，若出现过，当前节点路径与存在的路径id相同，并跳出循环，不写入表达式列表中；否则，写入
		 			if(pathInfo.get(i).getPathexp() .equals(e.getPath())) {       //由于pathexp是new出来的字符串，所以不能用==比较，要用equals方法
		 				elementNode.setPathId(pathInfo.get(i).getPathID());
		 				pInfo.setPathID(pathInfo.get(i).getPathID());
		 				f = true;
		 				break;
		 			}	
		 		}
		    	
		    	if(!f) {
					pInfo.setPathID(pathID);
					elementNode.setPathId(pathID);
					pathID ++;
				}*/
		    	pInfo.setPathID(pathID);
				elementNode.setPathId(pathID);
				pathID ++;
		    	pathInfo.add(pInfo);
		    	 
		    	 int level = arg0.getPath().split("/").length - 1;
			     //System.out.println(e.getName() + "***" + nodeID + "***" + nid + "***" + level);
			     //设置节点信息
			     elementNode.setDocName(xmlName);
			     elementNode.setNodeID(nodeID);
			     elementNode.setElementName(e.getName());
			     elementNode.setStart(nid);
			     elementNode.setLevel(level);
			     //得不到元素节点的文本值，此时还未访问到
			     /*if(!(e.getTextTrim().equals(""))) {
			    	 elementNode.setValue(e.getText());
			     }*/
			     
			     List<Attribute> attributeSet = e.attributes();
			     for(Iterator<Attribute> j = attributeSet.iterator(); j.hasNext();) {
			     //for (Iterator<Attribute> j = e.attributeIterator(); j.hasNext();) {
			    	 //f = false;
			    	 XMLNode attributeNode = new XMLNode();
			    	 pInfo = new PathInfo();
			    	 Attribute att = j.next();
			    	 nid ++;
			    	 nodeID ++;
			    	 attributeNum ++ ;
			    	 //System.out.println(att.getPath());
			    	 level = att.getPath().split("/").length - 1;
			    	 attributeNode.setDocName(xmlName);
			    	 attributeNode.setElementName(att.getName());
			    	 attributeNode.setNodeID(nodeID);
			    	 attributeNode.setStart(nid);
			    	 attributeNode.setEnd(nid);
			    	 attributeNode.setLevel(level);
			    	 attributeNode.setValue(att.getValue());
			    	 //System.out.println("@" + att.getName() + ":" + att.getValue() + "***" + nodeID + "***" + nid + "***" + level);
			    	 pInfo.setDocName(xmlName);
			    	 pInfo.setNodeID(nodeID);
			    	 pInfo.setPathexp(att.getPath());
			    	 /*for(int i = 0;i<pathInfo.size();i++) {
						if(pathInfo.get(i).getPathexp().equals(att.getPath())) {
							attributeNode.setPathId(pathInfo.get(i).getPathID());
							pInfo.setPathID(pathInfo.get(i).getPathID());
							f = true;
							break;
						}	
					 }
			    	 if(!f) {
						pInfo.setPathID(pathID);
						attributeNode.setPathId(pathID);
						pathID++;
					 }*/
			    	 pInfo.setPathID(pathID);
					 attributeNode.setPathId(pathID);
					 pathID++;
			    	 attrInfo.add(attributeNode);
			    	 pathInfo.add(pInfo);  
			    	 //att.detach();
			    	 j.remove();
			     }
			     eleInfo.add(elementNode);
			     nid ++;
			     nodeID ++;
			     elementNum ++;
			     e.detach();
		    } 
		    
		    public void onEnd(ElementPath ep) {  
		        Element e = ep.getCurrent(); //获得当前节点
		        //System.out.println(ep.getPath());
		        int level = ep.getPath().split("/").length - 1;
		        System.out.println(e.getName() + "***" + nid + "***" + level);
		        int i = 0;
		        //设置元素节点的end值及value值，注意元素节点的文本值只能在onEnd函数中得到，因为onEnd相当于访问了节点的结束标签
		        for(Element ee : elementSet) {
		        	if(ee == e) {
		        		eleInfo.get(i).setEnd(nid);
		        		if(!(e.getTextTrim().equals(""))) {
					    	 eleInfo.get(i).setValue(e.getText());
					     }
		        		break;
		        	}
		        	i++;
		        }
		        nid++;
		        e.detach(); //记得从内存中移去   
		    }
		});
		
		try {
			Document doc = reader.read(new File(xmlName));
			//Element rootElement = doc.getRootElement();
			System.out.println("**********************************************");
			System.out.println("节点个数为：" + (nodeID - 1));
			System.out.println("元素节点个数为：" + elementNum);
			System.out.println("属性节点个数为：" + attributeNum);
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			
			FSDataOutputStream out1 = fs.create(new Path(filename1));                                  //create()方法每次在写文件时会覆盖原有的内容
			FSDataOutputStream out2 = fs.create(new Path(filename2));
			FSDataOutputStream out3 = fs.create(new Path(filename3));  
			
			//FSDataOutputStream out1 = fs.append(new Path(filename1));           //append()方法可以向hdfs的文件中追加内容而不覆盖原有的内容，但需要事先在hdfs-site.xml文件中配置dfs.support.append为true
		    //FSDataOutputStream out2 = fs.append(new Path(filename2));
			//FSDataOutputStream out3 = fs.append(new Path(filename3));
			//FSDataOutputStream out4 = fs.append(new Path(filename4));
			//nodePosition(rootElement);
			
			for(int i=0;i<eleInfo.size();i++){
				//eleInfo.get(i).docName = xmlName;
				//str = "element&" + eleInfo.get(i).docName + "&" + eleInfo.get(i).nodeID + "&" + eleInfo.get(i).pathID + "&" + eleInfo.get(i).elementName + "&" + eleInfo.get(i).start + "&" + eleInfo.get(i).end + "&" + eleInfo.get(i).level + "&" + eleInfo.get(i).textValue;
				eleInfo.get(i).setDocName(xmlName);
				//将列族名称也写入到hdfs中，方便后续插入到hbase中
				str = "element&" + eleInfo.get(i).getDocName() + "&" + eleInfo.get(i).getNodeID() + "&" + eleInfo.get(i).getPathId() + "&" + eleInfo.get(i).getElementName() + "&" + eleInfo.get(i).getStart() + "&" + eleInfo.get(i).getEnd() + "&" + eleInfo.get(i).getLevel() + "&" + eleInfo.get(i).getValue();
				out1.write(str.getBytes(), 0, str.getBytes().length);
				out1.write(nextLine,0,nextLine.length);
			}
			
			for(int i=0;i<attrInfo.size();i++){
				//attrInfo.get(i).docName = xmlName;
				//str = "attribute&" + attrInfo.get(i).docName + "&" + attrInfo.get(i).nodeID + "&" + attrInfo.get(i).pathID + "&" + attrInfo.get(i).attrName + "&" + attrInfo.get(i).start + "&" + attrInfo.get(i).end + "&" + attrInfo.get(i).level + "&"  + attrInfo.get(i).value;
				/*attrInfo[i].docName = xmlName;
				str = attrInfo[i].docName + "&" + attrInfo[i].nodeID + "&" + attrInfo[i].attrName + "&" + attrInfo[i].start + "&" + attrInfo[i].end + "&" + attrInfo[i].level + "&" + attrInfo[i].pathID + "&"  + attrInfo[i].value;*/
				attrInfo.get(i).setDocName(xmlName);
				str = "attribute&" + attrInfo.get(i).getDocName() + "&" + attrInfo.get(i).getNodeID() + "&" + attrInfo.get(i).getPathId() + "&" + attrInfo.get(i).getElementName() + "&" + attrInfo.get(i).getStart() + "&" + attrInfo.get(i).getEnd() + "&" + attrInfo.get(i).getLevel() + "&"  + attrInfo.get(i).getValue();
				out2.write(str.getBytes(), 0, str.getBytes().length);
				out2.write(nextLine,0,nextLine.length);
			}
			
			/*for(int i=0;i<tx;i++){
				textInfo[i].docName = xmlName;
				str = textInfo[i].docName + "&" + textInfo[i] .nodeID + "&" + textInfo[i].value + "&" + textInfo[i].start + "&" + textInfo[i].end + "&" + textInfo[i].level + "&" + textInfo[i].pathID;
				out3.write(str.getBytes(), 0, str.getBytes().length);
				out3.write(nextLine,0,nextLine.length);  
			}*/
			
			for(int i=0;i<pathInfo.size();i++){
				//pathInfo.get(i).docName = xmlName;
				//str = "path&" + pathInfo.get(i).docName + "&" + pathInfo.get(i).nodeID + "&" + pathInfo.get(i) .pathID + "&" + pathInfo.get(i).pathexp;
				pathInfo.get(i).setDocName(xmlName);
				str = "path&" + pathInfo.get(i).getDocName() + "&" + pathInfo.get(i).getNodeID() + "&" + pathInfo.get(i) .getPathID() + "&" + pathInfo.get(i).getPathexp();
				out3.write(str.getBytes(), 0, str.getBytes().length);
				out3.write(nextLine,0,nextLine.length);
			}
			
			out1.close();
			out2.close();
			out3.close();
			//out4.close();
			fs.close();
			
			/*System.out.println();
			System.out.println("*******************ElementInfo*************************");
			for(int i=0;i<eleInfo.size();i++){
				System.out.println(eleInfo.get(i));
			}
			
			System.out.println("*******************AttributeInfo*************************");
			for(int j=0;j<attrInfo.size();j++){
				System.out.println(attrInfo.get(j));
			}
			System.out.println();
			System.out.println("*******************TextInfo*************************");
			for(int j=0;j<tx;j++){
				System.out.println(textInfo[j].docName + "," + textInfo[j] .nodeID + "," + textInfo[j].value + "," + textInfo[j].start + "," + textInfo[j].end + "," + textInfo[j].level + "," + textInfo[j].pathID);
			}
			
			System.out.println("*******************PathInfo*************************");
			for(PathInfo p : pathInfo){
				System.out.println(p);
			}*/
			
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
