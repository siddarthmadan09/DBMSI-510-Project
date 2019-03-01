package global;

public class Intervaltype {
	int start;
	int end;
	
	int START_MIN_VALUE = -100000;
	int END_MIN_VALUE = -100000;
	int START_MAX_VALUE = -100000;
	int END_MAX_VALUE = -100000;
	
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
	
	public void assign(int a, int b) {
		if (a > START_MIN_VALUE && a < START_MAX_VALUE && b > END_MIN_VALUE && b < END_MAX_VALUE) {
		this.start = a;
		this.end = b;
		} else {
		throw new IllegalStateException();
		}
	}
}
