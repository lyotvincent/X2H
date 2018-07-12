package hit.lqr.model;

public class PathInfo {
	private int nodeID;
	private String docName;
	private int pathID;
	private String pathexp;
	
	
	
	public int getNodeID() {
		return nodeID;
	}



	public void setNodeID(int nodeID) {
		this.nodeID = nodeID;
	}



	public String getDocName() {
		return docName;
	}



	public void setDocName(String docName) {
		this.docName = docName;
	}



	public int getPathID() {
		return pathID;
	}



	public void setPathID(int pathID) {
		this.pathID = pathID;
	}



	public String getPathexp() {
		return pathexp;
	}



	public void setPathexp(String pathexp) {
		this.pathexp = pathexp;
	}



	@Override
	public String toString() {
		return "PathInfo [nodeID=" + nodeID + ", docName=" + docName
				+ ", pathID=" + pathID + ", pathexp=" + pathexp + "]";
	}

}

