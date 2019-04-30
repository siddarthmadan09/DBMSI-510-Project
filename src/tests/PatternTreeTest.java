package tests;


import iterator.Iterator;
import iterator.NestedLoopsJoinsIndexScan;
import iterator.QueryPlanExecutor;
import iterator.SortMerge;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import java.nio.file.NoSuchFileException;

import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;

class GetInput {
    GetInput() {}
   
    /*
     * Gets validated integer choice from System.in
     */
    public static int getChoice (int min, int max) {
      
      BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
      int choice = -1;
      boolean validChoice = false;

      
      while(!validChoice) {
          try {
              choice = Integer.parseInt(in.readLine());
          }
          catch (NumberFormatException e) {
              return -1;
          }
          catch (IOException e) {
              return -1;
          }
          
          if( choice >= min && choice <= max) {
              break;
          } else {
              System.out.println("Invalid choice. Please enter again: ");
          }   
      }
      return choice;
    }
    
    /*
     * Gets validated String from System.in
     */
    public static String getString () {
        
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        String input = "";
        
        boolean validChoice = false;
        
        while(!validChoice) {
        
            try {
              input = in.readLine();
            }
            catch (IOException e) {
              return "";
            }
            
            if(input.length() != 0) {
                break;
            } else {
                System.out.println("Invalid choice. Please enter again: ");
            }
        }
        return input;
      } 

    /*
     * Gets Gets return value of System.in
     */
    public static void getReturn () {
      
      BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
      
      try {
        String ret = in.readLine();
      }
      catch (IOException e) {}
    } 
  }


/*
 * Pattern Tree Driver: 
 *      - Inputs an XML path, parses the file and adds the contents into the database.
 *      - Inputs and runs a Choice driven simple parse tree or complex parse tree. 
 */
class PTDriver implements GlobalConst {
    private boolean OK = true;
    private boolean FAIL = false;
    private Heapfile heap;
    private SystemDefs sysdef;
    public PCounter pcounter = PCounter.getSingletonInstance();

    public static final String INDEXNAME = "BTreeIndexForNLJ";
    
    public SystemDefs getSysdef() {
        return sysdef;
    }

    public void setSysdef(SystemDefs sysdef) {
        this.sysdef = sysdef;
    }

    /*
     * Parse input XML file and add contents into a new heap file.
     */
    public PTDriver(String path, final String heapFileName) {
        
        pcounter.resetAllCount();
        String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
        String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        boolean status;
        try {
          Runtime.getRuntime().exec(remove_logcmd);
          Runtime.getRuntime().exec(remove_dbcmd);
          Runtime.getRuntime().exec(remove_joincmd);
        }
        catch (IOException e) {
          System.err.println (""+e);
        }

        this.sysdef = new SystemDefs( dbpath, 100000, NUMBUF, "Clock" );
        
        this.heap = null;
        try {
          this.heap = new Heapfile(heapFileName);
        }
        catch (Exception e) {
          System.err.println("*** error in Heapfile constructor ***");
          status = FAIL;
          e.printStackTrace();
        }

        List<NodeTuple> nodes = null;
        try {
            nodes = global.ParseXML.parse(path);
        } catch (FileNotFoundException e) {
            System.out.println("Error: Invalid file path in xml input file");
        }
        catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        
        try {
            status = this.heap.insertRecordfromXML(nodes);
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException | HFException
                | HFBufMgrException | HFDiskMgrException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        
        pcounter.printThenResetCounters();
        System.out.println("Exporting done!");
        System.out.println("Creating index.");
//        createIndex();
        System.out.println("Index creation done.");
        pcounter.printThenResetCounters();
    }
    
    public void createIndex() {

        // Creating Index on heapfile
        AttrType[] Stypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString) };

        short[] Ssizes = new short[1];
        Ssizes[0] = 10;

        // _______________________________________________________________
        // *******************create an scan on the heapfile**************
        // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        // create a tuple of appropriate size

        Tuple tt = new Tuple();
        try {
            tt.setHdr((short) 3, Stypes, Ssizes);
        } catch (Exception e) {
            e.printStackTrace();
        }

        int sizett = tt.size();
        tt = new Tuple(sizett);
        try {
            tt.setHdr((short) 3, Stypes, Ssizes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Scan scan = null;

        try {
            scan = new Scan(heap);
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        try {
            btf = new BTreeFile(INDEXNAME, AttrType.attrString, 8, 1);
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        RID rid = new RID();
        String key = null;
        Tuple temp = null;

        try {
            temp = scan.getNext(rid);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (temp != null) {
            tt.tupleCopy(temp);

            try {
                key = tt.getStrFld(3);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                btf.insert(new StringKey(key.trim()), rid);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // close the file scan
        scan.closescan();
    }
    
    private void menuPatternTreeIp() {
        System.out.println("\n1. Run simple pattern tree");
        System.out.println("2. Run complex pattern tree");
        System.out.println("3. Exit");
        System.out.println("Enter your choice: ");
        
    }
    
    private void menuQueryPlans() {
        System.out.println("\n1. Run Query 1 of pattern tree");
        System.out.println("2. Run Query 2 of pattern tree");
        System.out.println("3. Run Query 3 of pattern tree");
        System.out.println("4. Input Another Pattern Tree");
        System.out.println("5. Exit");
        System.out.println("Enter your choice: ");
        
    }
    
    /*
     * Runs choice driven tests for running multiple simple parse trees
     * or multiple complex parse trees using selection of various query plansion in thread "main" java.lang.Error: Unresolved compilation problems: 
    Syntax error, insert "VariableDeclarators" to complete LocalVariableDeclaration
    Syntax error, insert ";" to complete LocalVariableDeclarationStatement
    Sys cannot be resolved

    at tests.PTDriver.createIndex(PatternTreeTest.java:196)
    at tests.PTDriver.<init>(PatternTreeTest.java:171)
    at tests.PatternTreeTest.main(PatternTreeTest.java:407)

     * 
     * TODO: Create an only argument based running of tests.
     */
    public void runTests (Boolean noArgs, String [] args, SystemDefs sysdefs, String HEAPFILENAME) {
        
        QueryPlanExecutor query = new QueryPlanExecutor();
        
        while(true) {
            
            menuPatternTreeIp();
            int flag = 0;
            int choice = GetInput.getChoice(1,3);
            switch(choice) {
            
                // Running simple pattern tree
                case 1: 
                    System.out.println("Enter simple pattern tree file path: ");
                    String ptPath = GetInput.getString();
                    SimplePatternTreeParser spt = new SimplePatternTreeParser(ptPath);
                    if (spt.getConditions() == null) {
                        break;
                    }
                    while(true) {
                        menuQueryPlans();
                        int choice2 = GetInput.getChoice(1,5);
                        if (choice2 == 4) {
                            break;
                        }
                     
                        // Running query plans 1, 2 or 3
                        Iterator it = null;
                        NestedLoopsJoinsIndexScan  nlj = null;
                        switch(choice2) {
                            case 1:
                                System.out.println("Running query plan 1");
                                
                                it = (NestedLoopsJoinsIndexScan)it;
                                it = query.QueryPlanExecutor1_iterative(spt.getMap(), spt.getConditions(), spt.getInl(),0,HEAPFILENAME,-1,spt.getDynamic());
                                break;
                            case 2:
                            System.out.println("Running query plan 2");
                            it = (SortMerge)it;
                            it=query.QueryPlanExecutor2_iterative(spt.getMap(), spt.getConditions(), spt.getInl(),0,HEAPFILENAME,-1,spt.getDynamic());
                        
        //                      QueryPlans.query2(spt.getConditions());
                                break;
                            case 3:
                                System.out.println("Running query plan 3");
                                it = (SortMerge)it;
                                it=query.QueryPlanExecutor3(spt.getMap(), spt.getConditions(), spt.getInl(),0,HEAPFILENAME,-1,spt.getDynamic());
        //                      QueryPlans.query3(spt.getConditions());
                                break;
                            case 5:
                                System.exit(0);   
                                break;
                            default:
                                it = (NestedLoopsJoinsIndexScan)it;
                                it = query.QueryPlanExecutor1_iterative(spt.getMap(), spt.getConditions(), spt.getInl(),0,HEAPFILENAME,-1,spt.getDynamic());
                                nlj = (NestedLoopsJoinsIndexScan)it;                        }
                        
                      

                            
                        
                      int sizeofTuple = it.getFinalTupleSize();
                      
                      AttrType []  outputtype = new AttrType[sizeofTuple];
                        
                      for(int i=0;i< sizeofTuple;i=i+3) {
                          outputtype[i]= new AttrType(AttrType.attrInterval);
                          outputtype[i+1]=new AttrType(AttrType.attrInteger);
                          outputtype[i+2]=new AttrType(AttrType.attrString);
                            
                        }
                    
                      Tuple t;
                        t = null;
                        try {
                          while ((t = it.get_next()) != null) {
                            t.print(outputtype);
                          }
                        }
                        catch (Exception e) {
                          System.err.println (""+e);
                          e.printStackTrace();
                          Runtime.getRuntime().exit(1);
                        }
        
                        System.out.println ("\n"); 
                        try {
                          it.close();
                        }
                        catch (Exception e) {
                        
                          e.printStackTrace();
                        }
                        pcounter.printThenResetCounters();
                    }
                    
                    
                    
                    break;
                
                // Running complex pattern tree
                case 2:
                    System.out.println("Enter complex pattern tree file path: ");
                    String complexPtPath = GetInput.getString();
                    ComplexPatternTreeParser cpt = new ComplexPatternTreeParser(complexPtPath);
                    // Passing empty page replacement policy will pick up the previous replacement policy
//                    sysdefs.recreateBM(cpt.getBuf_size(), "");
                    while(true) {
                        menuQueryPlans();
                        int choice2 = GetInput.getChoice(1,5);
                        if (choice2 == 4) {
                            flag  = 1;
                            break;
                        }
                     
                        // Running query plans 1, 2 or 3
                        switch(choice2) {
                            case 1:
                                System.out.println("Running query1");
                                cpt.execute2(1);
                                break;
                            case 2:
                                System.out.println("Running query2");
                                cpt.execute2(2);
                                break;
                            case 3:
                                System.out.println("Running query3");
                                cpt.execute2(3);
                                break;
                            case 5:
                                System.exit(0);
                                break;
                            default:
                                cpt.execute2(2);

                                System.out.println("Invalid choice. Enter choice again.");
                        }
                        break;
                    }
                    break;
                case 3:
                    System.exit(0);
                    break;
                default:
                    System.exit(0); 
            }
        }
        
    }
}


public class PatternTreeTest {

    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        String xmlPath;
        if (args.length == 0) {
            System.out.println("Enter xml input file path: ");
            xmlPath = GetInput.getString();
        } else {
            xmlPath = args[0];
        }
        
        File tmpDir = new File(xmlPath);
        
        while (!tmpDir.exists()) {
            System.out.println("XML input path does not exist. Please enter valid xml input file path: ");
            xmlPath = GetInput.getString();
            tmpDir = new File(xmlPath);
        }
        
        Boolean noArgs = (args.length == 1 ? true : false);

        System.out.println("Parsing and exporting xml file in database...");
        
        final String HEAPFILENAME = "xml.in";
        
        //Parse the input xml and add the data into the database.
        PTDriver pttest = new PTDriver(xmlPath, HEAPFILENAME);
        
        pttest.runTests(noArgs, args, pttest.getSysdef(),HEAPFILENAME);
        
    }

}