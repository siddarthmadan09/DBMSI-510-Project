package heap;

import global.Intervaltype;
import org.w3c.dom.Element;

public class NodeTuple extends Tuple {
	private Element nodeTag;
	private Intervaltype nodeIntLabel;
	
	public Element getNodeTag() {
		return nodeTag;
	}
	
	public void setNodeTag(Element nodeTag) {
		this.nodeTag = nodeTag;
	}
	
	public Intervaltype getNodeIntLabel() {
		return nodeIntLabel;
	}
	
	public void setNodeIntLabel(Intervaltype nodeIntLabel) {
		this.nodeIntLabel = nodeIntLabel;
	}
	
	public void convertNodeTupleToTuple() {
		
	}
}