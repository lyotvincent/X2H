package hit.lqr.service.storage;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;


public class NodePosition {
	public static int pid = 1;                       //记录路径表达式的序号
	private static int begin = 0;                     //记录节点在xml树中先序遍历的开始序号
	private static int end = 0;                       //记录节点在xml树中先序遍历的结束序号
	private static int count = 0;                      
	private static int id =0;                          //记录节点的id
	private static int level = 1;                      //节点在xml树中的层号
	private static int k = 0, att = 0 ,tx = 0;
	private static String exp = "";
	
	/*public static ElementInfo[] eleInfo = new ElementInfo[40000];                      //四个结构体数组分别用来存放元素节点信息、属性节点信息、文本节点信息、路径信息
	public static AttributeInfo[] attrInfo = new AttributeInfo[30000];
	public static TextInfo[] textInfo = new TextInfo[30000];
	public static PathInfo[] pathInfo = new PathInfo[22000];*/
	
	private List<ElementInfo> eleInfo = new ArrayList<ElementInfo>();
	private List<AttributeInfo> attrInfo = new ArrayList<AttributeInfo>();
	private List<PathInfo> pathInfo = new ArrayList<PathInfo>();
	
	public void info2HDFS(String filename1, String filename2, String filename3) {
		byte[] nextLine = "\n".getBytes();
		String str;
        //String xmlName = "/public2/home/Vincent/test/input/test1.xml";
		//String xmlName = "/public2/home/Vincent/test/XMLTest/input/uniprot_sprot.xml";
		String xmlName = "/public2/home/Vincent/test/XMLTest/input/uniprot1.xml";
		
		SAXReader reader = new SAXReader();
		try {
			Document doc = reader.read(new File(xmlName));
			Element rootElement = doc.getRootElement();
			
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			//String filename1 = "hdfs://localhost:9000/user/hadoop/element.txt";             //要写入的hdfs文件路径
			//String filename2 = "hdfs://localhost:9000/user/hadoop/attribute.txt";
			//String filename3 = "hdfs://localhost:9000/user/hadoop/text.txt";
			//String filename4 = "hdfs://localhost:9000/user/hadoop/path.txt";
			
			FSDataOutputStream out1 = fs.create(new Path(filename1));                                  //create()方法每次在写文件时会覆盖原有的内容
			FSDataOutputStream out2 = fs.create(new Path(filename2));
			FSDataOutputStream out3 = fs.create(new Path(filename3));
			//FSDataOutputStream out4 = fs.create(new Path(filename4));  
			
			//FSDataOutputStream out1 = fs.append(new Path(filename1));           //append()方法可以向hdfs的文件中追加内容而不覆盖原有的内容，但需要事先在hdfs-site.xml文件中配置dfs.support.append为true
		    //FSDataOutputStream out2 = fs.append(new Path(filename2));
			//FSDataOutputStream out3 = fs.append(new Path(filename3));
			//FSDataOutputStream out4 = fs.append(new Path(filename4));
			
			//System.out.println(pid);
			nodePosition(rootElement);
			
			for(int i=0;i<eleInfo.size();i++){
				eleInfo.get(i).docName = xmlName;
				//将列族名称也写入到hdfs中，方便后续插入到hbase中
				str = "element&" + eleInfo.get(i).docName + "&" + eleInfo.get(i).nodeID + "&" + eleInfo.get(i).pathID + "&" + eleInfo.get(i).elementName + "&" + eleInfo.get(i).start + "&" + eleInfo.get(i).end + "&" + eleInfo.get(i).level + "&" + eleInfo.get(i).textValue;
			    
				//str = eleInfo.get(i).docName + "&" + eleInfo.get(i).nodeID + "&" + eleInfo.get(i).pathID + "&" + eleInfo.get(i).elementName + "&" + eleInfo.get(i).start + "&" + eleInfo.get(i).end + "&" + eleInfo.get(i).level + "&" + eleInfo.get(i).textValue;
				/*eleInfo[i].docName = xmlName;
				str = eleInfo[i].docName + "&" + eleInfo[i].nodeID + "&" + eleInfo[i].elementName + "&" + eleInfo[i].start + "&" + eleInfo[i].end + "&" + eleInfo[i].level+ "&" + eleInfo[i].pathID;*/
				out1.write(str.getBytes(), 0, str.getBytes().length);
				out1.write(nextLine,0,nextLine.length);
			}
			
			for(int i=0;i<attrInfo.size();i++){
				attrInfo.get(i).docName = xmlName;
				str = "attribute&" + attrInfo.get(i).docName + "&" + attrInfo.get(i).nodeID + "&" + attrInfo.get(i).pathID + "&" + attrInfo.get(i).attrName + "&" + attrInfo.get(i).start + "&" + attrInfo.get(i).end + "&" + attrInfo.get(i).level + "&"  + attrInfo.get(i).value;
				/*attrInfo[i].docName = xmlName;
				str = attrInfo[i].docName + "&" + attrInfo[i].nodeID + "&" + attrInfo[i].attrName + "&" + attrInfo[i].start + "&" + attrInfo[i].end + "&" + attrInfo[i].level + "&" + attrInfo[i].pathID + "&"  + attrInfo[i].value;*/
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
				pathInfo.get(i).docName = xmlName;
				str = "path&" + pathInfo.get(i).docName + "&" + pathInfo.get(i).nodeID + "&" + pathInfo.get(i) .pathID + "&" + pathInfo.get(i).pathexp;
				out3.write(str.getBytes(), 0, str.getBytes().length);
				out3.write(nextLine,0,nextLine.length);
			}
			
			out1.close();
			out2.close();
			out3.close();
			//out4.close();
			fs.close();
			
			System.out.println();
			System.out.println("*******************ElementInfo*************************");
			for(int i=0;i<eleInfo.size();i++){
				System.out.println(eleInfo.get(i));
			}
			
			System.out.println("*******************AttributeInfo*************************");
			for(int j=0;j<attrInfo.size();j++){
				System.out.println(attrInfo.get(j));
			}
			/*System.out.println();
			System.out.println("*******************TextInfo*************************");
			for(int j=0;j<tx;j++){
				System.out.println(textInfo[j].docName + "," + textInfo[j] .nodeID + "," + textInfo[j].value + "," + textInfo[j].start + "," + textInfo[j].end + "," + textInfo[j].level + "," + textInfo[j].pathID);
			}*/
			
			System.out.println("*******************PathInfo*************************");
			for(PathInfo p : pathInfo){
				System.out.println(p);
			}
			
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
		 
		
        }
	
	//注意：别忘了添加其他三个表的pathID及docID
	public void nodePosition(Element node) {           //遍历xml文件得到各节点信息
		int tmp = 0;
		int kk = 0;
		boolean f = false;                      //用于判断表达式是否重复
		
		try{
			id++;       //记录先序遍历节点的编号
			count++;
			begin = count;
			//p(id + " " + node.getName() + ":" + begin + " " + level +" ");
			ElementInfo eInfo = new ElementInfo();
			eInfo.nodeID = id;
			eInfo.elementName = node.getName();
			eInfo.start = begin;
			eInfo.level = level;
			/*eleInfo[k] = new ElementInfo();                //首先要new一下，不然会报空指针错误
			eleInfo[k].nodeID = id;
			eleInfo[k].elementName = node.getName();
			eleInfo[k].start = begin;
			eleInfo[k].level = level;*/
			//kk = k;                                       //暂时保存当前节点在元素结构体数组中的序号
			//p(node.getPath());
			
			PathInfo pInfo = new PathInfo();
			pInfo.pathexp = "";
			pInfo.nodeID = id;
			/*pathInfo[pid] = new PathInfo();
			pathInfo[pid].pathexp = "";*/
			exp = node.getPath();
			pInfo.pathexp = exp;
			//此处可能会存在问题，集合中pid位置还没有值
			for(int i=0;i<pathInfo.size();i++) {
			//for(int i = 1;i<=pid;i++) {       //判断当前表达式在表达式列表中是否出现过，若出现过，当前节点路径与存在的路径id相同，并跳出循环，不写入表达式列表中；否则，写入
				if(pathInfo.get(i).pathexp .equals(exp)) {       //由于pathexp是new出来的字符串，所以不能用==比较，要用equals方法
					eInfo.pathID = pathInfo.get(i).pathID;
					pInfo.pathID = pathInfo.get(i).pathID;
					f = true;
					break;
				}	
			}
			
			if(!f) {
				//pathInfo.get(pid).pathexp = exp;
				//pathInfo.get(pid).pathID = pid;
				pInfo.pathID = pid;
				eInfo.pathID = pid;
				pid++;
			}
			
			pathInfo.add(pInfo);
			
			for (Iterator<Attribute> j = node.attributeIterator(); j.hasNext();) {              //得到该节点下的属性节点信息
		            Attribute attribute = j.next();
		            id++;
		            count++;
		            begin = count;                                 //属性节点的开始编号和结束编号相同
		            end = begin;
		            //p(id +   " " + "@" + attribute.getName()+ ":" + begin + " " + end + " " + (level+1) + " ");
		            AttributeInfo aInfo = new AttributeInfo();
		            aInfo.nodeID = id;
		            aInfo.attrName = attribute.getName();
		            aInfo.start = begin;
		            aInfo.end = end;
		            aInfo.level = level + 1;                 
		            aInfo.value = attribute.getValue();
		            /*attrInfo[att] = new AttributeInfo();
		            attrInfo[att].nodeID = id;
		            attrInfo[att].attrName = attribute.getName();
		            attrInfo[att].start = begin;
		            attrInfo[att].end = end;
		            attrInfo[att].level = level + 1;                 
		            attrInfo[att].value = attribute.getValue();*/
		            //att++;
		            //p(attribute.getPath());
		            
		            pInfo = new PathInfo();
		            pInfo.nodeID = id;
		            pInfo.pathexp = "";
		            exp = attribute.getPath();
		            pInfo.pathexp = exp;
		            for(int i = 0;i<pathInfo.size();i++) {
						if(pathInfo.get(i).pathexp.equals(exp)) {
							aInfo.pathID = pathInfo.get(i).pathID;
							pInfo.pathID = pathInfo.get(i).pathID;
							f = true;
							break;
						}	
					}
					
					if(!f) {
						//pathInfo[pid].pathexp = exp;
						pInfo.pathID = pid;
						aInfo.pathID = pid;
						pid++;
					}
					 //att++;	
					attrInfo.add(aInfo);
					pathInfo.add(pInfo);
			    	
		            //p(attribute.getName() + ":" +attribute.getValue());
		           // do something
		        }
			
			
			if(!(node.getTextTrim().equals(""))) {                    //得到该节点下的文本节点，文本节点的路径和其父节点（即该节点）相同
				  /*id++;
				  count++;
				  begin = count;
		    	  //p("(" + node.getText() + ")" + ":" + begin + " ");
		    	  count++;
		    	  end = count;
		    	  p(id + " " + "(" + node.getText() + ")" + ":" + begin + " " + end +" " +(level+1) + " ");
		    	    textInfo[tx] = new TextInfo();
		    	    textInfo[tx].nodeID = id;
		            textInfo[tx].value = node.getText();
		            textInfo[tx].start = begin;
		            textInfo[tx].end = end;
		            textInfo[tx].level = level + 1;
		            textInfo[tx].pathID = eleInfo[k].pathID;
		            tx++;*/
				  eInfo.textValue = node.getText();
						  
		       }
			
			eleInfo.add(eInfo);
			kk = k;
			
			tmp = level;                                                         //暂时保存当前节点的层号，因为遍历完其子节点后还要返回该节点
			Iterator<Element> i = node.elementIterator();                       //递归遍历整个xml
			
			if(i.hasNext()) {                                                    //如果该节点下面有子节点，则层数加一
				level++;
				
			}
			
			while(i.hasNext()){
				
				Element element = i.next();
			       // do something
				k++;
			    nodePosition(element);
			    
			    
			}                                                    
			level = tmp;                                                          //回到该节点
			count++;
			end = count;
			//p(node.getName() + ":" + end + " ");
			eleInfo.get(kk).end = end;
			//eInfo[kk].end = end;
			//p(" " + end + "");
			
		
		}catch (Exception e) {
			e.printStackTrace();
		}
	}	
	
	
	public static void p(Object o) {
		System.out.print(o);
	}
			

}


class ElementInfo {               //类中可以设置相应的函数，比如getDocId(),setDocId()等等
	public int nodeID;
	public String elementName;
	public String docName;
	public int pathID;
	public int start;
	public int end;
	public int level;
	String textValue;
	@Override
	public String toString() {
		return "ElementInfo [nodeID=" + nodeID + ", elementName=" + elementName
				+ ", docName=" + docName + ", pathID=" + pathID + ", start="
				+ start + ", end=" + end + ", level=" + level + ", textValue="
				+ textValue + "]";
	}
	
	
}

class AttributeInfo {
	public int nodeID;
	public String attrName;
	public String docName;
	public int pathID;
	public int start;
	public int end;
	public int level;
	public String value;
	@Override
	public String toString() {
		return "AttributeInfo [nodeID=" + nodeID + ", attrName=" + attrName
				+ ", docName=" + docName + ", pathID=" + pathID + ", start="
				+ start + ", end=" + end + ", level=" + level + ", value="
				+ value + "]";
	}
	
	
    
}

/*class TextInfo {
	public int nodeID;
	public String value;
	public String docName;
	public int pathID;
	public int start;
	public int end;
	public int level;
    
}*/

class PathInfo {
	int nodeID;
	public String docName;
	public int pathID;
	public String pathexp;
	@Override
	public String toString() {
		return "PathInfo [nodeID=" + nodeID + ", docName=" + docName
				+ ", pathID=" + pathID + ", pathexp=" + pathexp + "]";
	}
	
	
}
