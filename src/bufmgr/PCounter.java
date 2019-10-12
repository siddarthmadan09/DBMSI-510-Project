package bufmgr;

import java.util.HashSet;
import java.util.Set;

public class PCounter {
    
    private int diskReadCounter = 0;
    private int diskWriteCounter = 0;
    private Set<Integer> uniqueFrames = new HashSet<Integer>();
    private static PCounter pcounter = null;
         
        // other fields / standard constructors / getters
         
    public static PCounter getSingletonInstance() {
        if (pcounter == null) {
            pcounter = new PCounter();
        }
        return pcounter;
    }
    
    private PCounter() {}
      
    public void resetAllCount() {
        resetBuffCount();
        resetDiskCount();
    }
    public void resetBuffCount() { 
        uniqueFrames.clear();
    }
    public void resetDiskCount() { 
        diskReadCounter = 0;
        diskWriteCounter = 0;
    }
      
    /** Increments the read count of a certain frame page when the
       * page is pinned.
       *
       */
    public void incrementBuffReadCount(int frameNo) {
        uniqueFrames.add(frameNo);
      }
    
    public void incrementDiskReadCount() {
        diskReadCounter += 1;
      }
    
    public void incrementDiskWriteCount() {
        diskWriteCounter += 1;
      }
    /**
     * @return the incremented read count.
     */

    public int getBuffReadCounter() {
        return uniqueFrames.size();
    }

    public int getDiskReadCounter() {
        return diskReadCounter;
    }

    public int getDiskWriteCounter() {
        return diskWriteCounter;
    }
    public void printAllCounters() {
        System.out.println("-----Bufffer Frames Count: "+ getBuffReadCounter() + " Disk Read Count "+ getDiskReadCounter() +" Disk Write Count "+ getDiskWriteCounter()+ " -----");
    }
    public void printThenResetCounters() {
        printAllCounters();
        resetAllCount();
    
        
    }

}