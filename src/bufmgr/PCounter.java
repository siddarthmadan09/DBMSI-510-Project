package bufmgr;

public class PCounter {
	public static int rcounter = 0; 
	public static int wcounter = 0;
	
	private static PCounter pcounter = null;
	     
	    // other fields / standard constructors / getters
	     
    public static PCounter getSingletonInstance() {
        if (pcounter == null) {
        	pcounter = new PCounter();
        }
        return pcounter;
    }
    
	public PCounter() {}
	  
	public static void reset() { 
		rcounter = 0;
	    wcounter = 0;
	}
	  
	/** Increments the read count of a certain frame page when the
	   * page is pinned.
	   *
	   */
	public static void readIncrement() {
	    rcounter += 1;
	  }
	/**
	 * @return the incremented read count.
	 */
	public static int getRcounter() {
	    return rcounter;
	}
	  
	public static void writeIncrement() {
	    wcounter += 1;
	}
	  
	public static int getWcounter() {
	    return wcounter;
	}
}
