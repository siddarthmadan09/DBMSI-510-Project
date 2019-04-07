package tests;

import iterator.*;
import iterator.Iterator;
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


        this.sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );
        
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
     * or multiple complex parse trees using selection of various query plans
     * 
     * TODO: Create an only argument based running of tests.
     */
    public void runTests (Boolean noArgs, String [] args, SystemDefs sysdefs) {
        while(true) {
            menuPatternTreeIp();
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
                        switch(choice2) {
                            case 1:
                                System.out.println("Running query1");
        //                      QueryPlans.query1(spt.getConditions());
                                break;
                            case 2:
                                System.out.println("Running query2");
        //                      QueryPlans.query2(spt.getConditions());
                                break;
                            case 3:
                                System.out.println("Running query3");
        //                      QueryPlans.query3(spt.getConditions());
                                break;
                            case 5:
                                System.exit(0);   
                                break;
                            default:
                                System.out.println("Invalid choice. Enter choice again.");
                        }
                    }
                    break;
                
                // Running complex pattern tree
                case 2:
                    System.out.println("Enter complex pattern tree file path: ");
                    String complexPtPath = GetInput.getString();
                    ComplexPatternTreeParser spt2 = new ComplexPatternTreeParser(complexPtPath);
                    if (spt2.getConditions1() == null) {
                        break;
                    }
                    // Passing empty page replacement policy will pick up the previous replacement policy
                    sysdefs.recreateBM(spt2.getBuf_size(), "");
                    while(true) {
                        menuQueryPlans();
                        int choice2 = GetInput.getChoice(1,5);
                        if (choice2 == 4) {
                            break;
                        }
                     
                        // Running query plans 1, 2 or 3
                        switch(choice2) {
                            case 1:
                                System.out.println("Running query1");
        //                      QueryPlans.query1(spt2);
                                break;
                            case 2:
                                System.out.println("Running query2");
        //                      QueryPlans.query2(spt2);
                                break;
                            case 3:
                                System.out.println("Running query3");
        //                      QueryPlans.query3(spt2);
                                break;
                            case 5:
                                System.exit(0);
                                break;
                            default:
                                System.out.println("Invalid choice. Enter choice again.");
                        }
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
        
        System.out.println("Exporting done!");
        pttest.runTests(noArgs, args, pttest.getSysdef());
        
    }

}
