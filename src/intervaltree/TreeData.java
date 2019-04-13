package intervaltree;

import btree.DataClass;
import global.RID;

public class TreeData extends DataClass {

	private RID rid;

	public RID getData() {
		return new RID(rid.pageNo, rid.slotNo);
	}

	public void setData(RID rid) {
		this.rid = new RID(rid.pageNo, rid.slotNo);
	}

	public TreeData(RID rid) {
		super();
		this.rid = new RID(rid.pageNo, rid.slotNo);
	}

	@Override
	public String toString() {
		return "TreeData [rid=" + (new Integer(this.rid.pageNo.pid)).toString() +" "
	              + (new Integer(this.rid.slotNo)).toString() +  "]";
	}
	
	
}
