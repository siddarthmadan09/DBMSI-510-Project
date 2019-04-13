package intervaltree;

import btree.KeyClass;
import global.Intervaltype;

public class IntervalKey extends KeyClass {

	private Intervaltype key;
	
	public IntervalKey(Intervaltype interval) {
		super();
		this.key = new Intervaltype(interval);
	}

	
	
	public IntervalKey(int start, int end) {
		super();
		this.key = new Intervaltype(start,end);
		
	}



	@Override
	public String toString() {
		return "IntervalKey [interval=" + key + "]";
	}

	public Intervaltype getKey() {
		return new Intervaltype(key);
	}

	public void setKey(Intervaltype interval) {
		this.key = new Intervaltype(interval);
	}
}
