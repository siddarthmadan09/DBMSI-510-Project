package intervaltree;

import btree.KeyClass;
import global.Intervaltype;

public class IntervalKey extends KeyClass {

	private Intervaltype interval;
	
	public IntervalKey(Intervaltype interval) {
		super();
		this.interval = new Intervaltype(interval);
	}

	@Override
	public String toString() {
		return "IntervalKey [interval=" + interval + "]";
	}

	public Intervaltype getInterval() {
		return new Intervaltype(interval);
	}

	public void setInterval(Intervaltype interval) {
		this.interval = new Intervaltype(interval);
	}
}
