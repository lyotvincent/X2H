package hit.lqr.model;

import java.io.Serializable;

/*
 * 存储xml中的节点信息
 */

public class XMLNode implements Serializable{
	private int nodeID;
	private String elementName;
	private String docName;
	private int pathId;
	private int start;
	private int end;
	private int level;
	private String value;
	private int judge;
	
	
	
	public int getNodeID() {
		return nodeID;
	}
	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}
	public int getJudge() {
		return judge;
	}
	public void setJudge(int judge) {
		this.judge = judge;
	}
	public String getElementName() {
		return elementName;
	}
	public void setElementName(String elementName) {
		this.elementName = elementName;
	}
	public String getDocName() {
		return docName;
	}
	public void setDocName(String docName) {
		this.docName = docName;
	}
	public int getPathId() {
		return pathId;
	}
	public void setPathId(int pathId) {
		this.pathId = pathId;
	}
	public int getStart() {
		return start;
	}
	public void setStart(int start) {
		this.start = start;
	}
	public int getEnd() {
		return end;
	}
	public void setEnd(int end) {
		this.end = end;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "XMLNode [nodeID=" + nodeID + ",elementName=" + elementName 
				+ ", docName=" + docName + ", pathId=" + pathId + ", start=" + start + ", end=" + end
				+ ", level=" + level + ", value=" + value + ", judge=" + judge
				+ "]";
	}
	
	
	

}

