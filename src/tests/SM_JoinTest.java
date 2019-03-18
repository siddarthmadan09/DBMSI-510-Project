package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
 */

//	//Define the Sailor schema
//	class Sailor {
//		public int    sid;
//		public String sname;
//		public int    rating;
//		public double age;
//
//		public Sailor (int _sid, String _sname, int _rating,double _age) {
//			sid    = _sid;
//			sname  = _sname;
//			rating = _rating;
//			age    = _age;
//		}
//	}
//
//	//Define the Boat schema
//	class Boats {
//		public int    bid;
//		public String bname;
//		public String color;
//
//		public Boats (int _bid, String _bname, String _color) {
//			bid   = _bid;
//			bname = _bname;
//			color = _color;
//		}
//	}
//
//	//Define the Reserves schema
//	class Reserves {
//		public int    sid;
//		public int    bid;
//		public String date;
//
//		public Reserves (int _sid, int _bid, String _date) {
//			sid  = _sid;
//			bid  = _bid;
//			date = _date;
//		}
//	}

	class SM_JoinTest implements GlobalConst {

		private boolean OK = true;
		private boolean FAIL = false;
		private Vector sailors;
		private Vector boats;
		private Vector reserves;
		/** Constructor
		 */
		public SM_JoinTest(String path) {

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

		      SystemDefs sysdef = new SystemDefs( dbpath, 1000, GlobalConst.NUMBUF, "Clock" );
		      
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

		public boolean runTests() {

			Disclaimer();
			QueryXML();
			/*
    Query1();

    Query2();
    Query3();


    Query4();
    Query5();
    Query6();
			 */

			System.out.print ("Finished joins testing"+"\n");


			return true;
		}

		private void QueryXML_CondExpr(CondExpr[] expr) {

			expr[0].next  = null;
			expr[0].op    = new AttrOperator(AttrOperator.aopGT);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
			expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
			expr[0].flag=1;
			expr[1] = null;


		}

		private void Query1_CondExpr(CondExpr[] expr) {

			expr[0].next  = null;
			expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
			expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
			expr[1].next  = null;
			expr[1].type1 = new AttrType(AttrType.attrSymbol);
			expr[1].type2 = new AttrType(AttrType.attrInteger);
			expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
			expr[1].operand2.integer = 1;

			expr[2] = null;
		}



		private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

			expr[0].next  = null;
			expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
			expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr[1] = null;

			expr2[0].next  = null;
			expr2[0].op    = new AttrOperator(AttrOperator.aopEQ); 
			expr2[0].type1 = new AttrType(AttrType.attrSymbol);
			expr2[0].type2 = new AttrType(AttrType.attrSymbol);   
			expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
			expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
			expr2[1].next = null;
			expr2[1].type1 = new AttrType(AttrType.attrSymbol);
			expr2[1].type2 = new AttrType(AttrType.attrString);
			expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
			expr2[1].operand2.string = "red";

			expr2[2] = null;
		}

		private void Query3_CondExpr(CondExpr[] expr) {

			expr[0].next  = null;
			expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);
			expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);
			expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
			expr[1] = null;
		}

		private CondExpr[] Query5_CondExpr() {
			CondExpr [] expr2 = new CondExpr[3];
			expr2[0] = new CondExpr();


			expr2[0].next  = null;
			expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr2[0].type1 = new AttrType(AttrType.attrSymbol);

			expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
			expr2[0].type2 = new AttrType(AttrType.attrSymbol);

			expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr2[1] = new CondExpr();
			expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
			expr2[1].next = null;
			expr2[1].type1 = new AttrType(AttrType.attrSymbol);

			expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
			expr2[1].type2 = new AttrType(AttrType.attrReal);
			expr2[1].operand2.real = (float)40.0;


			expr2[1].next = new CondExpr();
			expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
			expr2[1].next.next = null;
			expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
			expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
			expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
			expr2[1].next.operand2.integer = 7;

			expr2[2] = null;
			return expr2;
		}

		private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

			expr[0].next  = null;
			expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr[0].type1 = new AttrType(AttrType.attrSymbol);

			expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
			expr[0].type2 = new AttrType(AttrType.attrSymbol);

			expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr[1].next  = null;
			expr[1].op    = new AttrOperator(AttrOperator.aopGT);
			expr[1].type1 = new AttrType(AttrType.attrSymbol);

			expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
			expr[1].type2 = new AttrType(AttrType.attrInteger);
			expr[1].operand2.integer = 7;

			expr[2] = null;

			expr2[0].next  = null;
			expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
			expr2[0].type1 = new AttrType(AttrType.attrSymbol);

			expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
			expr2[0].type2 = new AttrType(AttrType.attrSymbol);

			expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

			expr2[1].next = null;
			expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
			expr2[1].type1 = new AttrType(AttrType.attrSymbol);

			expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
			expr2[1].type2 = new AttrType(AttrType.attrString);
			expr2[1].operand2.string = "red";

			expr2[2] = null;
		}

		public void QueryXML() {

			System.out.print("**********************QueryXML strating *********************\n");
			boolean status = OK;
			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
			CondExpr [] leftFilter  = new CondExpr[2];
			leftFilter[0] = new CondExpr();

			leftFilter[0].next  = null;
			leftFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
			leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			leftFilter[0].type2 = new AttrType(AttrType.attrString);
			leftFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
			leftFilter[0].operand2.string = "class";

			leftFilter[1] = null;

			CondExpr [] rightFilter = new CondExpr[2];
			rightFilter[0] = new CondExpr();

			rightFilter[0].next  = null;
			rightFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
			rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
			rightFilter[0].type2 = new AttrType(AttrType.attrString);
			rightFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
			rightFilter[0].operand2.string = "stud";
			rightFilter[1] = null;

			CondExpr [] outFilter = new CondExpr[2];
			outFilter[0] = new CondExpr();

			QueryXML_CondExpr(outFilter);
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

			/*AttrType [] Btypes = {
	new AttrType(AttrType.attrInteger), 
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrString), 
      };

      short  []  Bsizes = new short[2];
      Bsizes[0] =30;
      Bsizes[1] =20;


      AttrType [] Jtypes = {
	new AttrType(AttrType.attrString), 
	new AttrType(AttrType.attrInteger), 
      };

      short  []  Jsizes = new short[1];
      Jsizes[0] = 30;
      AttrType [] JJtype = {
	new AttrType(AttrType.attrString), 
      };

      short [] JJsize = new short[1];
      JJsize[0] = 30; 

			 */

			FldSpec []  proj1 = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),
					new FldSpec(new RelSpec(RelSpec.innerRel), 1),
					new FldSpec(new RelSpec(RelSpec.innerRel), 2),
					new FldSpec(new RelSpec(RelSpec.innerRel), 3)
			}; // S.sname, R.bid

			/*FldSpec [] proj2  = {
	new FldSpec(new RelSpec(RelSpec.outer), 1)
      };
			 */
			FldSpec [] Sprojection = {
					new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2),
					new FldSpec(new RelSpec(RelSpec.outer), 3),

			};
			FldSpec [] Rprojection = {
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

			FileScan am2 = null;
			try {
				am2  = new FileScan("xml.in", Rtypes, Rsizes, 
						(short)3, (short)3,
						Rprojection, rightFilter);
			}
			catch (Exception e) {
				status = FAIL;
				System.err.println (""+e);
				e.printStackTrace();
			}

			SortMerge sm = null;
			try {
				// System.out.println(am.get_next().getStrFld(3));
				sm = new SortMerge(Stypes, 3, Ssizes,
						Rtypes, 3, Rsizes,
						1, 8, 
						1, 8, 
						10,
						am, am2, 
						false, false, ascending,
						outFilter, proj1, 6);
			}
			catch (Exception e) {
				System.err.println("*** join error in SortMerge constructor ***"); 
				status = FAIL;
				System.err.println (""+e);
				e.printStackTrace();
			}

			if (status != OK) {
				//bail out
				System.err.println ("*** Error constructing SortMerge");
				Runtime.getRuntime().exit(1);
			}



			//  QueryCheck qcheck1 = new QueryCheck(1);


			t = null;

			try {
				
				while ((t = sm.get_next()) != null) {
					t.print(JJtype);
					
					//System.out.println("Tuple "+JJ.getStrFld(3)+" "+t.getIntervalFld(1).getStart()+" "+t.getIntervalFld(1).getEnd()+" "+t.getLength());
					//qcheck1.Check(t);
				}
			}
			catch (Exception e) {
				System.err.println (""+e);
				e.printStackTrace();
				status = FAIL;
			}
			if (status != OK) {
				//bail out
				System.err.println ("*** Error in get next tuple ");
				Runtime.getRuntime().exit(1);
			}

			//qcheck1.report(1);
			try {
				sm.close();
			}
			catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
			System.out.println ("\n"); 
			if (status != OK) {
				//bail out
				System.err.println ("*** Error in closing ");
				Runtime.getRuntime().exit(1);
			}

			if (status != OK) {
				//bail out

				System.err.println ("*** Error setting up scan for sailors");
				Runtime.getRuntime().exit(1);
			}
		}  


		//   System.out.print( "After nested loop join S.sid|><|R.sid.\n");

		/*  NestedLoopsJoins nlj = null;
      try {
	nlj = new NestedLoopsJoins (Jtypes, 2, Jsizes,
				    Btypes, 3, Bsizes,
				    10,
				    inl, "boats.in",
				    outFilter2, null, proj2, 1);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for nested_loop_join");
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }

      System.out.print( "After nested loop join R.bid|><|B.bid AND B.color=red.\n");

      TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
      Sort sort_names = null;
      try {
	sort_names = new Sort (JJtype,(short)1, JJsize,
			       (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
      }
      catch (Exception e) {
	System.err.println ("*** Error preparing for sorting");
	System.err.println (""+e);
	Runtime.getRuntime().exit(1);
      }


      System.out.print( "After sorting the output tuples.\n");

		 */  
		//QueryCheck qcheck6 = new QueryCheck(6);



//
//		public void Query1() {
//
//			System.out.print("**********************Query1 strating *********************\n");
//			boolean status = OK;
//
//			// Sailors, Boats, Reserves Queries.
//			System.out.print ("Query: Find the names of sailors who have reserved "
//					+ "boat number 1.\n"
//					+ "       and print out the date of reservation.\n\n"
//					+ "  SELECT S.sname, R.date\n"
//					+ "  FROM   Sailors S, Reserves R\n"
//					+ "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");
//
//			System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");
//
//			CondExpr[] outFilter = new CondExpr[3];
//			outFilter[0] = new CondExpr();
//			outFilter[1] = new CondExpr();
//			outFilter[2] = new CondExpr();
//
//			Query1_CondExpr(outFilter);
//
//			Tuple t = new Tuple();
//
//			AttrType [] Stypes = new AttrType[4];
//			Stypes[0] = new AttrType (AttrType.attrInteger);
//			Stypes[1] = new AttrType (AttrType.attrString);
//			Stypes[2] = new AttrType (AttrType.attrInteger);
//			Stypes[3] = new AttrType (AttrType.attrReal);
//
//			//SOS
//			short [] Ssizes = new short[1];
//			Ssizes[0] = 30; //first elt. is 30
//
//			FldSpec [] Sprojection = new FldSpec[4];
//			Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//			Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//			Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
//			Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
//
//			FldSpec [] Rprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3)
//			}; 
//
//			CondExpr [] selects = new CondExpr [1];
//			selects = null;
//
//
//			FileScan am = null;
//			try {
//				am  = new FileScan("sailors.in", Stypes, Ssizes, 
//						(short)4, (short)4,
//						Sprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//
//			AttrType [] Rtypes = new AttrType[3];
//			Rtypes[0] = new AttrType (AttrType.attrInteger);
//			Rtypes[1] = new AttrType (AttrType.attrInteger);
//			Rtypes[2] = new AttrType (AttrType.attrString);
//
//			short [] Rsizes = new short[1];
//			Rsizes[0] = 15; 
//			FldSpec [] Rprojection1 = new FldSpec[3];
//			Rprojection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
//			Rprojection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//			Rprojection1[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
//
//			FileScan am2 = null;
//			try {
//				am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
//						(short)3, (short) 3,
//						Rprojection1, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for reserves");
//				Runtime.getRuntime().exit(1);
//			}
//
//
//			FldSpec [] proj_list = new FldSpec[2];
//			proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
//			proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
//
//			AttrType [] jtype = new AttrType[2];
//			jtype[0] = new AttrType (AttrType.attrString);
//			jtype[1] = new AttrType (AttrType.attrString);
//
//			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//			SortMerge sm = null;
//			try {
//				sm = new SortMerge(Stypes, 4, Ssizes,
//						Rtypes, 3, Rsizes,
//						1, 4, 
//						1, 4, 
//						10,
//						am, am2, 
//						false, false, ascending,
//						outFilter, proj_list, 2);
//			}
//			catch (Exception e) {
//				System.err.println("*** join error in SortMerge constructor ***"); 
//				status = FAIL;
//				System.err.println (""+e);
//				e.printStackTrace();
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error constructing SortMerge");
//				Runtime.getRuntime().exit(1);
//			}
//
//
//
//			QueryCheck qcheck1 = new QueryCheck(1);
//
//
//			t = null;
//
//			try {
//				while ((t = sm.get_next()) != null) {
//					t.print(jtype);
//
//					qcheck1.Check(t);
//				}
//			}
//			catch (Exception e) {
//				System.err.println (""+e);
//				e.printStackTrace();
//				status = FAIL;
//			}
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error in get next tuple ");
//				Runtime.getRuntime().exit(1);
//			}
//
//			qcheck1.report(1);
//			try {
//				sm.close();
//			}
//			catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//			System.out.println ("\n"); 
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error in closing ");
//				Runtime.getRuntime().exit(1);
//			}
//		}
//
//		public void Query2() {}
//
//
//		public void Query3() {
//			System.out.print("**********************Query3 strating *********************\n"); 
//			boolean status = OK;
//
//			// Sailors, Boats, Reserves Queries.
//
//			System.out.print 
//			( "Query: Find the names of sailors who have reserved a boat.\n\n"
//					+ "  SELECT S.sname\n"
//					+ "  FROM   Sailors S, Reserves R\n"
//					+ "  WHERE  S.sid = R.sid\n\n"
//					+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");
//
//			CondExpr [] outFilter = new CondExpr[2];
//			outFilter[0] = new CondExpr();
//			outFilter[1] = new CondExpr();
//
//			Query3_CondExpr(outFilter);
//
//			Tuple t = new Tuple();
//			t = null;
//
//			AttrType Stypes[] = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrReal)
//			};
//			short []   Ssizes = new short[1];
//			Ssizes[0] = 30;
//
//			AttrType [] Rtypes = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//			};
//			short  []  Rsizes = new short[1];
//			Rsizes[0] =15;
//
//			FldSpec [] Sprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3),
//					new FldSpec(new RelSpec(RelSpec.outer), 4)
//			};
//
//			CondExpr[] selects = new CondExpr [1];
//			selects = null;
//
//			iterator.Iterator am = null;
//			try {
//				am  = new FileScan("sailors.in", Stypes, Ssizes,
//						(short)4, (short) 4,
//						Sprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//
//			FldSpec [] Rprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3)
//			}; 
//
//			iterator.Iterator am2 = null;
//			try {
//				am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
//						(short)3, (short)3,
//						Rprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for reserves");
//				Runtime.getRuntime().exit(1);
//			}
//
//			FldSpec [] proj_list = {
//					new FldSpec(new RelSpec(RelSpec.outer), 2)
//			};
//
//			AttrType [] jtype     = { new AttrType(AttrType.attrString) };
//
//			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//			SortMerge sm = null;
//			try {
//				sm = new SortMerge(Stypes, 4, Ssizes,
//						Rtypes, 3, Rsizes,
//						1, 4,
//						1, 4,
//						20,
//						am, am2,
//						false, false, ascending,
//						outFilter, proj_list, 1);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error constructing SortMerge");
//				Runtime.getRuntime().exit(1);
//			}
//
//			QueryCheck qcheck3 = new QueryCheck(3);
//
//
//			t = null;
//
//			try {
//				while ((t = sm.get_next()) != null) {
//					t.print(jtype);
//					qcheck3.Check(t);
//				}
//			}
//			catch (Exception e) {
//				System.err.println (""+e);
//				e.printStackTrace();
//				Runtime.getRuntime().exit(1);
//			}
//
//
//			qcheck3.report(3);
//
//			System.out.println ("\n"); 
//			try {
//				sm.close();
//			}
//			catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//		}
//
//		public void Query4() {
//			System.out.print("**********************Query4 strating *********************\n");
//			boolean status = OK;
//
//			// Sailors, Boats, Reserves Queries.
//
//			System.out.print 
//			("Query: Find the names of sailors who have reserved a boat\n"
//					+ "       and print each name once.\n\n"
//					+ "  SELECT DISTINCT S.sname\n"
//					+ "  FROM   Sailors S, Reserves R\n"
//					+ "  WHERE  S.sid = R.sid\n\n"
//					+ "(Tests FileScan, Projection, Sort-Merge Join and "
//					+ "Duplication elimination.)\n\n");
//
//			CondExpr [] outFilter = new CondExpr[2];
//			outFilter[0] = new CondExpr();
//			outFilter[1] = new CondExpr();
//
//			Query3_CondExpr(outFilter);
//
//			Tuple t = new Tuple();
//			t = null;
//
//			AttrType Stypes[] = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrReal)
//			};
//			short []   Ssizes = new short[1];
//			Ssizes[0] = 30;
//
//			AttrType [] Rtypes = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//			};
//			short  []  Rsizes = new short[1];
//			Rsizes[0] =15;
//
//			FldSpec [] Sprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3),
//					new FldSpec(new RelSpec(RelSpec.outer), 4)
//			};
//
//			CondExpr[] selects = new CondExpr [1];
//			selects = null;
//
//			iterator.Iterator am = null;
//			try {
//				am  = new FileScan("sailors.in", Stypes, Ssizes,
//						(short)4, (short) 4,
//						Sprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//
//			FldSpec [] Rprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3)
//			}; 
//
//			iterator.Iterator am2 = null;
//			try {
//				am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
//						(short)3, (short)3,
//						Rprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for reserves");
//				Runtime.getRuntime().exit(1);
//			}
//
//			FldSpec [] proj_list = {
//					new FldSpec(new RelSpec(RelSpec.outer), 2)
//			};
//
//			AttrType [] jtype     = { new AttrType(AttrType.attrString) };
//
//			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//			SortMerge sm = null;
//			short  []  jsizes    = new short[1];
//			jsizes[0] = 30;
//			try {
//				sm = new SortMerge(Stypes, 4, Ssizes,
//						Rtypes, 3, Rsizes,
//						1, 4,
//						1, 4,
//						10,
//						am, am2,
//						false, false, ascending,
//						outFilter, proj_list, 1);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error constructing SortMerge");
//				Runtime.getRuntime().exit(1);
//			}
//
//
//			DuplElim ed = null;
//			try {
//				ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
//			}
//			catch (Exception e) {
//				System.err.println (""+e);
//				Runtime.getRuntime().exit(1);
//			}
//
//			QueryCheck qcheck4 = new QueryCheck(4);
//
//
//			t = null;
//
//			try {
//				while ((t = ed.get_next()) != null) {
//					t.print(jtype);
//					qcheck4.Check(t);
//				}
//			}
//			catch (Exception e) {
//				System.err.println (""+e);
//				e.printStackTrace(); 
//				Runtime.getRuntime().exit(1);
//			}
//
//			qcheck4.report(4);
//			try {
//				ed.close();
//			}
//			catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//			System.out.println ("\n");  
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//		}
//
//		public void Query5() {
//			System.out.print("**********************Query5 strating *********************\n");  
//			boolean status = OK;
//			// Sailors, Boats, Reserves Queries.
//
//			System.out.print 
//			("Query: Find the names of old sailors or sailors with "
//					+ "a rating less\n       than 7, who have reserved a boat, "
//					+ "(perhaps to increase the\n       amount they have to "
//					+ "pay to make a reservation).\n\n"
//					+ "  SELECT S.sname, S.rating, S.age\n"
//					+ "  FROM   Sailors S, Reserves R\n"
//					+ "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
//					+ "(Tests FileScan, Multiple Selection, Projection, "
//					+ "and Sort-Merge Join.)\n\n");
//
//
//			CondExpr [] outFilter;
//			outFilter = Query5_CondExpr();
//
//			Tuple t = new Tuple();
//			t = null;
//
//			AttrType Stypes[] = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrReal)
//			};
//			short []   Ssizes = new short[1];
//			Ssizes[0] = 30;
//
//			AttrType [] Rtypes = {
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrInteger),
//					new AttrType(AttrType.attrString),
//			};
//			short  []  Rsizes = new short[1];
//			Rsizes[0] = 15;
//
//			FldSpec [] Sprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3),
//					new FldSpec(new RelSpec(RelSpec.outer), 4)
//			};
//
//			CondExpr[] selects = new CondExpr [1];
//			selects[0] = null;
//
//			FldSpec [] proj_list = {
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3),
//					new FldSpec(new RelSpec(RelSpec.outer), 4)
//			};
//
//			FldSpec [] Rprojection = {
//					new FldSpec(new RelSpec(RelSpec.outer), 1),
//					new FldSpec(new RelSpec(RelSpec.outer), 2),
//					new FldSpec(new RelSpec(RelSpec.outer), 3)
//			};
//
//			AttrType [] jtype     = { 
//					new AttrType(AttrType.attrString), 
//					new AttrType(AttrType.attrInteger), 
//					new AttrType(AttrType.attrReal)
//			};
//
//
//			iterator.Iterator am = null;
//			try {
//				am  = new FileScan("sailors.in", Stypes, Ssizes, 
//						(short)4, (short)4,
//						Sprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for sailors");
//				Runtime.getRuntime().exit(1);
//			}
//
//			iterator.Iterator am2 = null;
//			try {
//				am2 = new FileScan("reserves.in", Rtypes, Rsizes, 
//						(short)3, (short)3,
//						Rprojection, null);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error setting up scan for reserves");
//				Runtime.getRuntime().exit(1);
//			}
//
//			TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
//			SortMerge sm = null;
//			try {
//				sm = new SortMerge(Stypes, 4, Ssizes,
//						Rtypes, 3, Rsizes,
//						1, 4,
//						1, 4,
//						10,
//						am, am2,
//						false, false, ascending,
//						outFilter, proj_list, 3);
//			}
//			catch (Exception e) {
//				status = FAIL;
//				System.err.println (""+e);
//			}
//
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error constructing SortMerge");
//				Runtime.getRuntime().exit(1);
//			}
//
//			QueryCheck qcheck5 = new QueryCheck(5);
//			//Tuple t = new Tuple();
//			t = null;
//
//			try {
//				while ((t = sm.get_next()) != null) {
//					t.print(jtype);
//					qcheck5.Check(t);
//				}
//			}
//			catch (Exception e) {
//				System.err.println (""+e);
//				Runtime.getRuntime().exit(1);
//			}
//
//			qcheck5.report(5);
//			try {
//				sm.close();
//			}
//			catch (Exception e) {
//				status = FAIL;
//				e.printStackTrace();
//			}
//			System.out.println ("\n"); 
//			if (status != OK) {
//				//bail out
//				System.err.println ("*** Error close for sortmerge");
//				Runtime.getRuntime().exit(1);
//			}
//		}
//
//		public void Query6(){}


		private void Disclaimer() {
			System.out.print ("\n\nAny resemblance of persons in this database to"
					+ " people living or dead\nis purely coincidental. The contents of "
					+ "this database do not reflect\nthe views of the University,"
					+ " the Computer  Sciences Department or the\n"
					+ "developers...\n\n");
		}



	public static void main(String argv[])
	{
		boolean sortstatus;
		//SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
		//JavabaseDB.openDB("/tmp/nwangdb", 5000);

	    String path = "/Users/apurvabharatia/Documents/xml_sample_data.xml";
		SM_JoinTest jjoin = new SM_JoinTest(path);

		sortstatus = jjoin.runTests();
		
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		}
		else {
			System.out.println("join tests completed successfully");
		}
	}
}

