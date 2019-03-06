package global;

public class Intervaltype {
	int start;
	int end;
	
	static int START_MIN_VALUE = -100000;
	static int END_MIN_VALUE = -100000;
	static int START_MAX_VALUE = -100000;
	static int END_MAX_VALUE = -100000;
	
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
	
	public static Intervaltype min_value() {
		Intervaltype type = new Intervaltype();
		type.setStart(START_MIN_VALUE);
		type.setEnd(END_MIN_VALUE);
		return type;
	}
	
	public static Intervaltype max_value() {
		Intervaltype type = new Intervaltype();
		type.setStart(START_MAX_VALUE);
		type.setEnd(END_MAX_VALUE);
		return type;
	}
}
