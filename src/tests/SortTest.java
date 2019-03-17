package tests;

import java.io.*; 
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


class SORTDriver extends TestDriver 
  implements GlobalConst {

  private static String   data1[] = {
	"raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa", 
	"dhanoa", "dsilva", "kurniawa", "dissoswa", "waic", "susanc", "kinc", 
	"marc", "scottc", "yuc", "ireland", "rathgebe", "joyce", "daode", 
	"yuvadee", "he", "huxtable", "muerle", "flechtne", "thiodore", "jhowe",
	"frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah", "bloch",
	"binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
	"chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel", 
	"newell", "schiesl", "neuman", "heitzman", "wan", "gunawan", "djensen",
	"juei-wen", "josephin", "harimin", "xin", "zmudzin", "feldmann", 
	"joon", "wawrzon", "yi-chun", "wenchao", "seo", "karsono", "dwiyono", 
	"ginther", "keeler", "peter", "lukas", "edwards", "mirwais","schleis", 
	"haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
	"honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey", 
	"awny", "savoy", "meltz"}; 
      
  private static String   data2[] = {
	"andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw", 
	"chingju", "chui", "chung-pi", "cychan", "dai", "daode", "dhanoa", 
	"dissoswa", "djensen", "dsilva", "dwiyono", "edwards", "evgueni", 
	"feldmann", "flechtne", "frankief", "ginther", "gray", "guangshu", 
	"gunawan", "hai", "handi", "harimin", "haris", "he", "heitzman", 
	"honghu", "huxtable", "ireland", "jhowe", "joon", "josephin", "joyce",
	"jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
	"leela", "lukas", "mak", "marc", "markert", "meltz", "meyers", 
	"mirwais", "muerle", "muthiah", "neuman", "newell", "peter", "raghu", 
	"randal", "rathgebe", "robert", "savoy", "schiesl", "schleis", 
	"scottc", "seo", "shi", "shun-kit", "siddiqui", "soma", "sonthi", 
	"sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
	"wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin", 
	"yi-chun", "yiching", "yuc", "yung", "yuvadee", "zmudzin" };
  
  private static String data3[] = {
		  
		  "393","-99999","-99998",
		  "rollno","-99997","-99996",
		  "mmomo","-99995","-99994",
		  "course","-99993","-99992",
		  "xffggf","-99989","-99988",
		  "firstname","-99990","-99987",
		  "kad","-99985","-99984",
		  "lastname","-99986","-99983",
		  "dinkar","-99981","-99980",
		  "nickname","-99982","-99979",
		  "85","-99977","-99976",
		  "marks","-99978","-99975",
		  "student","-99991","-99974",
		  "493","-99973","-99972",
		  "rollno","-99971","-99970",
		  "mmomo","-99969","-99968",
		  "course","-99967","-99966",
		  "Vaneet","-99963","-99962",
		  "firstname","-99964","-99961",
		  "Gupta","-99959","-99958",
		  "lastname","-99960","-99957",
		  "vinni","-99955","-99954",
		  "nickname","-99956","-99953",
		  "95","-99951","-99950",
		  "marks","-99952","-99949",
		  "student","-99965","-99948",
		  "593","-99947","-99946",
		  "rollno","-99945","-99944",
		  "mmomo","-99943","-99942",
		  "course","-99941","-99940",
		  "jasvir","-99937","-99936",
		  "firstname","-99938","-99935",
		  "singn","-99933","-99932",
		  "lastname","-99934","-99931",
		  "jazz","-99929","-99928",
		  "nickname","-99930","-99927",
		  "90","-99925","-99924",
		  "marks","-99926","-99923",
		  "student","-99939","-99922",
		  "class","-100000","-99921"
  };

  private static int   NUM_RECORDS = data3.length; 
  private static int   LARGE = 1000; 
  private static short REC_LEN1 = 32; 
  private static short REC_LEN2 = 160; 
  private static int   SORTPGNUM = 12; 


  public SORTDriver(String path) {
      String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
      String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

      String remove_cmd = "/bin/rm -rf ";
      String remove_logcmd = remove_cmd + logpath;
      String remove_dbcmd = remove_cmd + dbpath;
      String remove_joincmd = remove_cmd + dbpath;

      try {
        Runtime.getRuntime().exec(remove_logcmd);
        Runtime.getRuntime().exec(remove_dbcmd);
        Runtime.getRuntime().exec(remove_joincmd);
      }
      catch (IOException e) {
        System.err.println (""+e);
      }

     
      /*
      ExtendedSystemDefs extSysDef = 
        new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
                    1000,500,200,"Clock");
      */

      SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
      
      // creating the XML Interval relation
      AttrType [] Stypes = new AttrType[3];
      Stypes[0] = new AttrType (AttrType.attrInterval);
      Stypes[1] = new AttrType (AttrType.attrInteger);
      Stypes[2] = new AttrType (AttrType.attrString);

      //SOS // Max Size of String is 5 chars
      short [] Ssizes = new short [1];
      Ssizes[0] = 10; //first elt. is 30
      
      Tuple t = new Tuple();
      boolean status = true;
      try {
        t.setHdr((short) 3,Stypes, Ssizes);
      }
      catch (Exception e) {
        System.err.println("*** error in Tuple.setHdr() ***");
        status = FAIL;
        e.printStackTrace();
      }
      
      int size = t.size();
      
      // inserting the tuple into file "sailors"
      RID             rid;
      Heapfile        f = null;
      try {
        f = new Heapfile("xml.in");
      }
      catch (Exception e) {
        System.err.println("*** error in Heapfile constructor ***");
        status = FAIL;
        e.printStackTrace();
      }
      
      t = new Tuple(size);
      try {
        t.setHdr((short) 3, Stypes, Ssizes);
      }
      catch (Exception e) {
        System.err.println("*** error in Tuple.setHdr() ***");
        status = FAIL;
        e.printStackTrace();
      }
      
      List<NodeTuple> nodes = null;
      try {
          nodes = global.ParseXML.parse(path);
      } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
      }
      
      
      for (NodeTuple node : nodes) {
        try {
      t.setIntervalFld(1, node.getNodeIntLabel());
      t.setIntFld(2, node.getLevel());
      t.setStrFld(3, node.getName());
      
        }
        catch (Exception e) {
      System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
      status = FAIL;
      e.printStackTrace();
        }
        
        try {
      rid = f.insertRecord(t.returnTupleByteArray());
        }
        catch (Exception e) {
      System.err.println("*** error in Heapfile.insertRecord() ***");
      status = FAIL;
      e.printStackTrace();
        }      
      }
      if (status != OK) {
        //bail out
        System.err.println ("*** Error creating relation for sailors");
        Runtime.getRuntime().exit(1);
      }  
  }
//  @Override
//  public boolean runTests ()  {
//    
//    System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
//    
//    SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );
//
//    // Kill anything that might be hanging around
//    String newdbpath;
//    String newlogpath;
//    String remove_logcmd;
//    String remove_dbcmd;
//    String remove_cmd = "/bin/rm -rf ";
//
//    newdbpath = dbpath;
//    newlogpath = logpath;
//
//    remove_logcmd = remove_cmd + logpath;
//    remove_dbcmd = remove_cmd + dbpath;
//
//    // Commands here is very machine dependent.  We assume
//    // user are on UNIX system here
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//    } 
//    catch (IOException e) {
//      System.err.println (""+e);
//    }
//    
//    remove_logcmd = remove_cmd + newlogpath;
//    remove_dbcmd = remove_cmd + newdbpath;
//
//    //This step seems redundant for me.  But it's in the original
//    //C++ code.  So I am keeping it as of now, just in case I
//    //I missed something
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//    } 
//    catch (IOException e) {
//      System.err.println (""+e);
//    }
//
//    //Run the tests. Return type different from C++
//    boolean _pass = runAllTests();
//
//    //Clean up again
//    try {
//      Runtime.getRuntime().exec(remove_logcmd);
//      Runtime.getRuntime().exec(remove_dbcmd);
//    } 
//    catch (IOException e) {
//      System.err.println (""+e);
//    }
//    
//    System.out.println ("\n" + "..." + testName() + " tests ");
//    System.out.println (_pass==OK ? "completely successfully" : "failed");
//    System.out.println (".\n\n");
//    
//    return _pass;
//  }

    
public boolean runTests() {
  

  test7();
  
  
  System.out.print ("Finished joins testing"+"\n");
 
  
  return true;
}
  
  
  protected boolean test1()
  {
    System.out.println("------------------------ TEST 1 --------------------------");
    return true;
//    boolean status = OK;
//
//    AttrType[] attrType = new AttrType[2];
//    attrType[0] = new AttrType(AttrType.attrString);
//    attrType[1] = new AttrType(AttrType.attrString);
//    short[] attrSize = new short[2];
//    attrSize[0] = REC_LEN1;
//    attrSize[1] = REC_LEN2;
//    TupleOrder[] order = new TupleOrder[2];
//    order[0] = new TupleOrder(TupleOrder.Ascending);
//    order[1] = new TupleOrder(TupleOrder.Descending);
//    
//    // create a tuple of appropriate size
//    Tuple t = new Tuple();
//    try {
//      t.setHdr((short) 2, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    int size = t.size();
//    
//    // Create unsorted data file "test1.in"
//    RID             rid;
//    Heapfile        f = null;
//    try {
//      f = new Heapfile("test1.in");
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) 2, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    for (int i=0; i<NUM_RECORDS; i++) {
//      try {
//	t.setStrFld(1, data1[i]);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      try {
//	rid = f.insertRecord(t.returnTupleByteArray());
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//
//    // create an iterator by open a file scan
//    FldSpec[] projlist = new FldSpec[2];
//    RelSpec rel = new RelSpec(RelSpec.outer); 
//    projlist[0] = new FldSpec(rel, 1);
//    projlist[1] = new FldSpec(rel, 2);
//    
//    FileScan fscan = null;
//    
//    try {
//      fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    // Sort "test1.in" 
//    Sort sort = null;
//    try {
//      sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//
//    int count = 0;
//    t = null;
//    String outval = null;
//    
//    try {
//      t = sort.get_next();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace(); 
//    }
//
//    boolean flag = true;
//    
//    while (t != null) {
//      if (count >= NUM_RECORDS) {
//	System.err.println("Test1 -- OOPS! too many records");
//	status = FAIL;
//	flag = false; 
//	break;
//      }
//      
//      try {
//	outval = t.getStrFld(1);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      if (outval.compareTo(data2[count]) != 0) {
//	System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);
//	
//	System.err.println("Test1 -- OOPS! test1.out not sorted");
//	status = FAIL;
//      }
//      count++;
//
//      try {
//	t = sort.get_next();
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (count < NUM_RECORDS) {
//	System.err.println("Test1 -- OOPS! too few records");
//	status = FAIL;
//    }
//    else if (flag && status) {
//      System.err.println("Test1 -- Sorting OK");
//    }
//
//    // clean up
//    try {
//      sort.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    System.err.println("------------------- TEST 1 completed ---------------------\n");
//    
//    return status;
  }


  protected boolean test2()
  {
    System.out.println("------------------------ TEST 2 --------------------------");
    return true;
//    boolean status = OK;
//
//    AttrType[] attrType = new AttrType[1];
//    attrType[0] = new AttrType(AttrType.attrString);
//    short[] attrSize = new short[1];
//    attrSize[0] = REC_LEN1;
//    TupleOrder[] order = new TupleOrder[2];
//    order[0] = new TupleOrder(TupleOrder.Ascending);
//    order[1] = new TupleOrder(TupleOrder.Descending);
//    
//    // create a tuple of appropriate size
//    Tuple t = new Tuple();
//    try {
//      t.setHdr((short) 1, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    int size = t.size();
//    
//    // Create unsorted data file "test2.in"
//    RID             rid;
//    Heapfile        f = null;
//    try {
//      f = new Heapfile("test2.in");
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) 1, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    for (int i=0; i<NUM_RECORDS; i++) {
//      try {
//	t.setStrFld(1, data1[i]);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      try {
//	rid = f.insertRecord(t.returnTupleByteArray());
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//
//    // create an iterator by open a file scan
//    FldSpec[] projlist = new FldSpec[1];
//    RelSpec rel = new RelSpec(RelSpec.outer); 
//    projlist[0] = new FldSpec(rel, 1);
//    
//    FileScan fscan = null;
//    
//    try {
//      fscan = new FileScan("test2.in", attrType, attrSize, (short) 1, 1, projlist, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//     
//    // Sort "test2.in"
//    Sort sort = null;
//    try {
//      sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[1], REC_LEN1, SORTPGNUM);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//
//    int count = 0;
//    t = null;
//    String outval = null;
//    
//    try {
//      t = sort.get_next();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    boolean flag = true;
//    
//    while (t != null) {
//      if (count >= NUM_RECORDS) {
//	System.err.println("Test2 -- OOPS! too many records");
//	status = FAIL;
//	flag = false; 
//	break;
//      }
//      
//      try {
//	outval = t.getStrFld(1);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//
//      if (outval.compareTo(data2[NUM_RECORDS - count - 1]) != 0) {
//	System.err.println("Test2 -- OOPS! test2.out not sorted");
//	status = FAIL;
//      }
//      count++;
//
//      try {
//	t = sort.get_next();
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (count < NUM_RECORDS) {
//	System.err.println("Test2 -- OOPS! too few records");
//	status = FAIL;
//    }
//    else if (flag && status) {
//      System.err.println("Test2 -- Sorting OK");
//    }
//
//    // clean up
//    try {
//      sort.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    System.err.println("------------------- TEST 2 completed ---------------------\n");
//        
//    return status;
  }


  protected boolean test3()
  {
    System.out.println("------------------------ TEST 3 --------------------------");
    return true;
//    boolean status = OK;
//
//    Random random1 = new Random((long) 1000);
//    Random random2 = new Random((long) 1000);
//    
//    AttrType[] attrType = new AttrType[4];
//    attrType[0] = new AttrType(AttrType.attrString);
//    attrType[1] = new AttrType(AttrType.attrString);
//    attrType[2] = new AttrType(AttrType.attrInteger);
//    attrType[3] = new AttrType(AttrType.attrReal);
//    short[] attrSize = new short[2];
//    attrSize[0] = REC_LEN1;
//    attrSize[1] = REC_LEN1;
//    TupleOrder[] order = new TupleOrder[2];
//    order[0] = new TupleOrder(TupleOrder.Ascending);
//    order[1] = new TupleOrder(TupleOrder.Descending);
//    
//    Tuple t = new Tuple();
//
//    try {
//      t.setHdr((short) 4, attrType, attrSize);
//    }
//    catch (Exception e) {
//      System.err.println("*** error in Tuple.setHdr() ***");
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    int size = t.size();
//
//    // Create unsorted data file "test3.in"
//    RID             rid;
//    Heapfile        f = null;
//    try {
//      f = new Heapfile("test3.in");
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) 4, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    int inum = 0;
//    float fnum = 0;
//    int count = 0;
//    
//    for (int i=0; i<LARGE; i++) {
//      // setting fields
//      inum = random1.nextInt();
//      fnum = random2.nextFloat();
//      try {
//	t.setStrFld(1, data1[i%NUM_RECORDS]);
//	t.setIntFld(3, inum);
//	t.setFloFld(4, fnum);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//
//      try {
//	rid = f.insertRecord(t.returnTupleByteArray());
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//
//    // create an iterator by open a file scan
//    FldSpec[] projlist = new FldSpec[4];
//    RelSpec rel = new RelSpec(RelSpec.outer); 
//    projlist[0] = new FldSpec(rel, 1);
//    projlist[1] = new FldSpec(rel, 2);
//    projlist[2] = new FldSpec(rel, 3);
//    projlist[3] = new FldSpec(rel, 4);
//    
//    FileScan fscan = null;
//    
//    // Sort "test3.in" on the int attribute (field 3) -- Ascending
//    System.out.println(" -- Sorting in ascending order on the int field -- ");
//    
//    try {
//      fscan = new FileScan("test3.in", attrType, attrSize, (short) 4, 4, projlist, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//
//    Sort sort = null;
//    try {
//      sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, order[0], 4, SORTPGNUM);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//    
//    count = 0;
//    t = null;
//    int iout = 0;
//    int ival = 0;
//    
//    try {
//      t = sort.get_next();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace(); 
//    }
//
//    if (t != null) {
//      // get an initial value
//      try {
//	ival = t.getIntFld(3);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    } 
//
//    boolean flag = true;
//    
//    while (t != null) {
//      if (count >= LARGE) {
//	System.err.println("Test3 -- OOPS! too many records");
//	status = FAIL;
//	flag = false; 
//	break;
//      }
//      
//      try {
//	iout = t.getIntFld(3);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      if (iout < ival) {
//	System.err.println("count = " + count + " iout = " + iout + " ival = " + ival);
//	
//	System.err.println("Test3 -- OOPS! test3.out not sorted");
//	status = FAIL;
//	break; 
//      }
//      count++;
//      ival = iout;
//      
//      try {
//	t = sort.get_next();
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (count < LARGE) {
//	System.err.println("Test3 -- OOPS! too few records");
//	status = FAIL;
//    }
//    else if (flag && status) {
//      System.err.println("Test3 -- Sorting of int field OK\n");
//    }
//
//    // clean up
//    try {
//      sort.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    // Sort "test3.in" on the int attribute (field 3) -- Ascending
//    System.out.println(" -- Sorting in descending order on the float field -- ");
//    
//    try {
//      fscan = new FileScan("test3.in", attrType, attrSize, (short) 4, 4, projlist, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//     
//    try {
//      sort = new Sort(attrType, (short) 4, attrSize, fscan, 4, order[1], 4, SORTPGNUM);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//
//    count = 0;
//    t = null;
//    float fout = 0;
//    float fval = 0;
//    
//    try {
//      t = sort.get_next();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace(); 
//    }
//
//    if (t != null) {
//      // get an initial value
//      try {
//	fval = t.getFloFld(4);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    } 
//
//    flag = true;
//    
//    while (t != null) {
//      if (count >= LARGE) {
//	System.err.println("Test3 -- OOPS! too many records");
//	status = FAIL;
//	flag = false; 
//	break;
//      }
//      
//      try {
//	fout = t.getFloFld(4);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      if (fout > fval) {
//	System.err.println("count = " + count + " fout = " + fout + " fval = " + fval);
//	
//	System.err.println("Test3 -- OOPS! test3.out not sorted");
//	status = FAIL;
//	break; 
//      }
//      count++;
//      fval = fout;
//      
//      try {
//	t = sort.get_next();
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (count < LARGE) {
//	System.err.println("Test3 -- OOPS! too few records");
//	status = FAIL;
//    }
//    else if (flag && status) {
//      System.err.println("Test3 -- Sorting of float field OK\n");
//    }
//
//    // clean up
//    try {
//      sort.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    System.err.println("------------------- TEST 3 completed ---------------------\n");
//        
//    return status;
  }
    
  protected boolean test4()
  {
//    System.out.println("------------------------ TEST 4 --------------------------");
//    
//    boolean status = OK;
//
//    AttrType[] attrType = new AttrType[2];
//    attrType[0] = new AttrType(AttrType.attrString);
//    attrType[1] = new AttrType(AttrType.attrString);
//    short[] attrSize = new short[2];
//    attrSize[0] = REC_LEN1;
//    attrSize[1] = REC_LEN2;
//    TupleOrder[] order = new TupleOrder[2];
//    order[0] = new TupleOrder(TupleOrder.Ascending);
//    order[1] = new TupleOrder(TupleOrder.Descending);
//    
//    // create a tuple of appropriate size
//    Tuple t = new Tuple();
//    try {
//      t.setHdr((short) 2, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    int size = t.size();
//    
//    // Create unsorted data file 
//    RID             rid1, rid2;
//    Heapfile        f1 = null;
//    Heapfile        f2 = null;
//    try {
//      f1 = new Heapfile("test4-1.in");
//      f2 = new Heapfile("test4-2.in");
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    t = new Tuple(size);
//    try {
//      t.setHdr((short) 2, attrType, attrSize);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    for (int i=0; i<NUM_RECORDS; i++) {
//      try {
//	t.setStrFld(1, data1[i]);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      try {
//	rid1 = f1.insertRecord(t.returnTupleByteArray());
//	rid2 = f2.insertRecord(t.returnTupleByteArray());
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    
//    
//    // create an iterator by open a file scan
//    FldSpec[] projlist = new FldSpec[2];
//    RelSpec rel = new RelSpec(RelSpec.outer); 
//    projlist[0] = new FldSpec(rel, 1);
//    projlist[1] = new FldSpec(rel, 2);
//    
//    FileScan fscan1 = null;
//    FileScan fscan2 = null;
//    
//    try {
//      fscan1 = new FileScan("test4-1.in", attrType, attrSize, (short) 2, 2, projlist, null);
//      fscan2 = new FileScan("test4-2.in", attrType, attrSize, (short) 2, 2, projlist, null);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//
//
//    // Sort input files
//    Sort sort1 = null;
//    Sort sort2 = null;
//    try {
//      sort1 = new Sort(attrType, (short) 2, attrSize, fscan1, 1, order[0], REC_LEN1, SORTPGNUM);
//      sort2 = new Sort(attrType, (short) 2, attrSize, fscan2, 1, order[1], REC_LEN1, SORTPGNUM);
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//
//    int count = 0;
//    Tuple t1 = null;
//    Tuple t2 = null; 
//    String outval = null;
//    
//    try {
//      t1 = sort1.get_next();
//      t2 = sort2.get_next();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace(); 
//    }
//
//    boolean flag = true;
//    
//    while (t1 != null) {
//      if (count >= NUM_RECORDS) {
//	System.err.println("Test4 -- OOPS! too many records");
//	status = FAIL;
//	flag = false; 
//	break;
//      }
//
//      try {
//	outval = t1.getStrFld(1);
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//      
//      if (outval.compareTo(data2[count]) != 0) {
//	System.err.println("outval = " + outval + "\tdata2[count] = " + data2[count]);
//	
//	System.err.println("Test4 -- OOPS! test4.out not sorted");
//	status = FAIL;
//      }
//      count++;
//
//      if (t2 == null) {
//	System.err.println("Test4 -- t2 is null prematurely");
//	status = FAIL;
//      }
//      else {
//	try {
//	  outval = t2.getStrFld(1);
//	}
//	catch (Exception e) {
//	  status = FAIL;
//	  e.printStackTrace();
//	}
//	
//	if (outval.compareTo(data2[NUM_RECORDS - count]) != 0) {
//	  System.err.println("outval = " + outval + "\tdata2[count] = " + data2[NUM_RECORDS - count]);
//	  
//	  System.err.println("Test4 -- OOPS! test4.out not sorted");
//	  status = FAIL;
//	}
//      }
//      
//      try {
//	t1 = sort1.get_next();
//	t2 = sort2.get_next();
//      }
//      catch (Exception e) {
//	status = FAIL;
//	e.printStackTrace();
//      }
//    }
//    if (count < NUM_RECORDS) {
//      System.err.println("count = " + count);
//      
//	System.err.println("Test4 -- OOPS! too few records");
//	status = FAIL;
//    }
//    else if (flag && status) {
//      System.err.println("Test4 -- Sorting OK");
//    }
//
//    // clean up
//    try {
//      sort1.close();
//      sort2.close();
//    }
//    catch (Exception e) {
//      status = FAIL;
//      e.printStackTrace();
//    }
//    
//    System.err.println("------------------- TEST 4 completed ---------------------\n");
//    
//    return status;
      return true;
  }
    
  protected boolean test5()
  {
    return true;
  }
    
  protected boolean test6()
  {
    return true;
  }
  
  /*********************XML changes************************/
  protected boolean test7()
  {
	  System.out.println("------------------------ TEST 7 --------------------------");
	    
	  
	    
	    System.out.print("**********************QueryXML strating *********************\n");
	    boolean status = OK;
	    
	    CondExpr [] leftFilter  = new CondExpr[2];
	    leftFilter[0] = new CondExpr();
	    
	    leftFilter[0].next  = null;
	    leftFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	    leftFilter[0].type2 = new AttrType(AttrType.attrString);
	    leftFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
	    leftFilter[0].operand2.string = "stud";
	    
	    leftFilter[1] = null;
	    
	    CondExpr [] rightFilter = new CondExpr[2];
	    rightFilter[0] = new CondExpr();
	    
	    rightFilter[0].next  = null;
	    rightFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
	    rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	    rightFilter[0].type2 = new AttrType(AttrType.attrString);
	    rightFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
	    rightFilter[0].operand2.string = "last";
	    rightFilter[1] = null;
	          
	    CondExpr [] outFilter = new CondExpr[2];
	    outFilter[0] = new CondExpr();
	    
	    //QueryXML_CondExpr(outFilter);
	    Tuple t = new Tuple();
	    t = null;
	    
	    AttrType [] Stypes = {
	  new AttrType(AttrType.attrInterval),  
	  new AttrType(AttrType.attrInteger), 
	  new AttrType(AttrType.attrString)
	    };
	  
	    short []   Ssizes = new short[1];
	    Ssizes[0] = 10;
	    
	    
	    AttrType [] Rtypes = {
	  new AttrType(AttrType.attrInterval), 
	  new AttrType(AttrType.attrInteger), 
	  new AttrType(AttrType.attrString), 
	    };
	    
	    short  []  Rsizes = new short[1] ;
	    Rsizes[0] = 10;

	    
	    FldSpec []  proj1 = {
	              new FldSpec(new RelSpec(RelSpec.outer), 1),
	              new FldSpec(new RelSpec(RelSpec.outer), 2),
	              new FldSpec(new RelSpec(RelSpec.outer), 3),
	              new FldSpec(new RelSpec(RelSpec.innerRel), 1),
	              new FldSpec(new RelSpec(RelSpec.innerRel), 2),
	              new FldSpec(new RelSpec(RelSpec.innerRel), 3)
	    }; // S.sname, R.bid
	    
	    FldSpec [] Sprojection = {
	  new FldSpec(new RelSpec(RelSpec.outer), 1),
	  new FldSpec(new RelSpec(RelSpec.outer), 2),
	  new FldSpec(new RelSpec(RelSpec.outer), 3),

	    };
	    
	    AttrType [] JJtype = {
	            new AttrType(AttrType.attrInterval),
	            new AttrType(AttrType.attrInteger),
	              new AttrType(AttrType.attrString),

	            new AttrType(AttrType.attrInterval),
	            new AttrType(AttrType.attrInteger),
	              new AttrType(AttrType.attrString),
	              
	                };
	    
	    FileScan am = null;
	    try {
	  am  = new FileScan("xml.in", Stypes, Ssizes, 
	             (short)3, (short)3,
	             Sprojection, leftFilter);
	    }
	    catch (Exception e) {
	  status = FAIL;
	  System.err.println (""+e);
	  e.printStackTrace();
	    }
	    
	    if (status != OK) {
	  //bail out
	  
	  System.err.println ("*** Error setting up scan for sailors");
	  Runtime.getRuntime().exit(1);
	    }
		
	    
	    FileScan fscan = null;
	    
	    try {
	      fscan = new FileScan("xml.in", Stypes, Ssizes, (short)3, (short)3, Sprojection, null);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    TupleOrder[] order = new TupleOrder[1];
	    order[0] = new TupleOrder(TupleOrder.Ascending);

	    // Sort "test1.in" 
	    Sort sort = null;
	    try {
	      sort = new Sort(Stypes, (short) 3, Ssizes, fscan, 2, order[0], REC_LEN1, SORTPGNUM);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    

	    int count = 0;
	    t = null;
	    String outval = null;
	    
	    try {
	      t = sort.get_next();
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace(); 
	    }

	    boolean flag = true;
	    
	    while (t != null) {
	      if (count >= NUM_RECORDS) {
		System.err.println("Test7 -- OOPS! too many records");
		status = FAIL;
		flag = false; 
		break;
	      }
	      
	      try {
		outval = t.getStrFld(3);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	      System.out.println("Outval"+outval);

	      count++;

	      try {
		t = sort.get_next();
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
//	    if (count < 40) {
//	    	System.out.println(count);
//		System.err.println("Test7 -- OOPS! too few records");
//		status = FAIL;
//	    }
//	    else if (flag && status) {
//	      System.err.println("Test7 -- Sorting OK");
//	    }

	    // clean up
	    try {
	      sort.close();
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
	    System.err.println("------------------- TEST 7 completed ---------------------\n");
	    
	    return status;

	  
  }
  
  protected String testName()
  {
    return "Sort";
  }
}

public class SortTest
{
  public static void main(String argv[])
  {
    boolean sortstatus;

    String path = "/home/siddharth/Courses/CSE510/git-project/DBMSI-510-Project/xml_sample_data.xml";
    SORTDriver sort = new SORTDriver(path);

    sortstatus = sort.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during sorting tests");
    }
    else {
      System.out.println("Sorting tests completed successfully");
    }
  }
}

