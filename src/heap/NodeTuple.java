package heap;

import global.Intervaltype;
import org.w3c.dom.Element;

public class NodeTuple extends Tuple {
	private Element nodeTag;
	private Intervaltype nodeIntLabel;
	private int level;
	private String name;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

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
	
}