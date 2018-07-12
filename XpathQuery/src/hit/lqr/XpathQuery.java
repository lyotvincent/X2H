package hit.lqr;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.ElementHandler;
import org.dom4j.ElementPath;
import org.dom4j.Node;
import org.dom4j.XPath;
import org.dom4j.io.SAXReader;

/**
 * 该程序用来测试xpath语句查询的执行时间
 * @author LiuQiuru
 *
 */
public class XpathQuery {

	private static int n = 1;
	//private static int level = 1;
	private static ArrayList<Element> ele = new ArrayList<Element>();
	
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		String filename = "uniprot1.xml";
		//final String xpath = "//teachers[teacher/@id='001']";
		//String filename = "uniprot15.xml";
		/*if(args.length != 2) {
			System.err.println("参数个数必须为2！");
			System.exit(1);
		}
		String filename = args[0];
		String xpath = args[1];*/
		//String filename = "/public2/home/Vincent/test/XMLTest/input/uniprot_sprot.xml";
		//String xpath = "//entry[gene//@type='ORF']/protein//fullName";
		//String xpath = "//entry[organismHost/name='Ambystoma']/reference//citation[dbReference/@id='15165820']/authorList";
		//String xpath = "//reference/citation[authorList/person/@name='Tan W.G.']/dbReference[@type='DOI']/@id";
		//String xpath = "//entry[accession='Q6GZX4']/organism[name/@type='common']//lineage[taxon='Viruses']";
		String xpath = "//feature[@type='chain']";
		SAXReader sr = new SAXReader();
		
		/*sr.setDefaultHandler(new ElementHandler(){ 
		    public void onStart(ElementPath arg0) { 
		    	 Element e = arg0.getCurrent(); //获得当前节点 
		    	 //System.out.println(e.getName() + " " + e.getText());
		    	 ele.add(e);
		    	 System.out.println(arg0.getPath());
		    	 int level = arg0.getPath().split("/").length - 1;
			     System.out.println(e.getName() + "***" + n + "***" + level); 
			     List<Attribute> attributeSet = e.attributes();
			     for(Iterator<Attribute> j = attributeSet.iterator(); j.hasNext();) {
			     //for (Iterator<Attribute> j = e.attributeIterator(); j.hasNext();) {
			    	 Attribute att = j.next();
			    	 n ++;
			    	 System.out.println(arg0.getPath() + "/" + "@" + att.getName());
			    	 int attLevel = level + 1;
			    	 System.out.println("@" + att.getName() + ":" + att.getValue() + "***" + n + "***" + attLevel);
			    	 //att.detach();
			    	 j.remove();
			     }
			     n ++;
			     e.detach();
		    } 
		    
		    public void onEnd(ElementPath ep) {  
		        Element e = ep.getCurrent(); //获得当前节点
		        Document doc = e.getDocument();
		        //System.out.println(e.getName() + "---" + e.getText());
		        int i = 0;
		        for(Element ee : ele) {
		        	if(ee == e) {
		        		//System.out.println("@@@" + i);
		        	}
		        	i++;
		        }
		        Element parent = e.getParent();
		        //System.out.println(ep.getPath());
		        int level = ep.getPath().split("/").length - 1;
		        //System.out.println(e.getName() + "***" + n + "***" + level);
		        
		        n++;
		        e.detach(); //记得从内存中移去   
		    }
		});*/ 
	
		try {
			Document document = sr.read(new File(filename));
			//System.out.println(document.asXML());
		    /*XPath x = document.createXPath(xpath);
		    List<Node> result = x.selectNodes(document);*/
			List<Node> result = document.selectNodes(xpath);
		    System.out.println(result);
			
		    int i = 1;
		    //得到的节点可能为元素节点或属性节点，应该分情况输出
		    for(Node node : result) {
		    	//System.out.println(node.asXML());
		    	if(node.getNodeTypeName().equals("Attribute")) {
		    		System.out.println("@" + node.getName() + "=" + node.getStringValue());
		    	}else {
		    		Element e = (Element) node;
		    		String resultText = nodeInfo(e,i);
			    	System.out.println(resultText);
			    	i = 1;
		    	}
		    }
		    
		} catch (DocumentException e) {
			e.printStackTrace();
		}finally {
			long endTime = System.currentTimeMillis();
			System.out.println("运行时间为：" +  (endTime - startTime) + "ms");
		}

	}

	/**
	 *获得某个元素节点的详细信息，包括其子节点的信息
	 * @param e：元素节点
	 * @param printLevel：层次
	 * @return
	 */
	private static String nodeInfo(Element e,int printLevel) {
		String resultText = "";
		String printStr = "";
		int tmpLevel;
		
		for(int p =0;p<printLevel;p++) {                                                                                                                        //缩进，用于显示出节点之间的层次关系
    		printStr = printStr + "   ";
    	}
		
		String textValue = (e.getTextTrim()!="") ? e.getText() : "";
		resultText += (printStr + e.getName() + ": " + textValue);
		
		//获取该元素节点下的属性节点信息
		for(Iterator<Attribute> att=e.attributeIterator();att.hasNext();) {
			Attribute attribute = att.next();
			resultText += (" " + "@" + attribute.getName() + "=" + attribute.getValue());
		}
		//换行
		resultText += "\n";
		
		//递归遍历子节点
		Iterator<Element> ele = e.elementIterator();
		while(ele.hasNext()) {
			tmpLevel = printLevel;
			printLevel ++;
			Element childElement = ele.next();
			resultText += nodeInfo(childElement, printLevel);
			printLevel = tmpLevel;
		}
		
		return resultText;
	}

}
