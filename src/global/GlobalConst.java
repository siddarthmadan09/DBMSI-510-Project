package global;

import bufmgr.PCounter;

public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 8000;
  public static final int NUMBUF = 10000;

  /** Size of page. */
  public static final int MINIBASE_PAGESIZE = 8096;           // in bytes
  public static final int PAGE_METADATA_SIZE = 16;
  public static final int TUPLE_SIZE = 34;
  public static final int TUPLE_BUFFER_SIZE = 4;

  
  /** Size of each frame. */
  public static final int MINIBASE_BUFFER_POOL_SIZE = 8096;   // in Frames

  public static final int MAX_SPACE = 8096;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 100000000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    
  public static final int MAX_NAME = 50;

  public static final int INVALID_PAGE = -1;
  public static final PCounter pcounter = PCounter.getSingletonInstance();
}
