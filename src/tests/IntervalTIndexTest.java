package tests;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import btree.BTreeFile;
import btree.StringKey;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.IndexType;
import global.Intervaltype;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.NodeTuple;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import intervaltree.IntervalKey;
import intervaltree.IntervalTreeFile;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.NestedLoopsJoinsIndexScan;
import iterator.RelSpec;

public class IntervalTIndexTest {

	public static final String HEAPFILENAME = "xml2.in";
	public static final String INDEXNAME = "BTreeIndexForNLJ";

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
		Heapfile f = null;
		try {
			f = new Heapfile(HEAPFILENAME);
		} catch (Exception e) {
			e.printStackTrace();
		}

		Scan scan = null;

		try {
			scan = new Scan(f);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file
		IntervalTreeFile btf = null;
		try {
			btf = new IntervalTreeFile(INDEXNAME, AttrType.attrInterval, 8, 1);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		Intervaltype key = null;
		Tuple temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (temp != null) {
			tt.tupleCopy(temp);

			try {
				key = tt.getIntervalFld(1);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				btf.insert(new IntervalKey(key), rid);
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

	public IntervalTIndexTest(String XMLPath) {
		super();

		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.nljtestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".nljlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, GlobalConst.NUMBUF, "Clock");

		// creating the XML Interval relation
		AttrType[] Stypes = new AttrType[3];
		Stypes[0] = new AttrType(AttrType.attrInterval);
		Stypes[1] = new AttrType(AttrType.attrInteger);
		Stypes[2] = new AttrType(AttrType.attrString);

		// SOS // Max Size of String is 5 chars
		short[] Ssizes = new short[1];
		Ssizes[0] = 10; // first elt. is 30

		Tuple t = new Tuple();
		try {
			t.setHdr((short) Stypes.length, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			e.printStackTrace();
		}
		
		int size = t.size();
		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile(HEAPFILENAME);
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}
		t = new Tuple(size);
	    try {
	      t.setHdr((short) 3, Stypes, Ssizes);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
		System.out.println("Starting with the Parsing.....");
		List<NodeTuple> nodes = null;
		try {
			nodes = global.ParseXML.parse(XMLPath);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.out.println("Parsing Completed......");
		for (NodeTuple node : nodes) {
			try {
				t.setIntervalFld(1, node.getNodeIntLabel());
				t.setIntFld(2, node.getLevel());
				t.setStrFld(3, node.getName());

			} catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				e.printStackTrace();
			}
		}
		System.out.println("Inserted Records successfully...");

	}
	private void printAllIndex() {

		AttrType[] ltypes = new AttrType[3];
			ltypes[0] = new AttrType(AttrType.attrInterval);
			ltypes[1] = new AttrType(AttrType.attrInteger);
			ltypes[2] = new AttrType(AttrType.attrString);
		

		short[] lsizes = new short[1];
			lsizes[0] = 10;


		FldSpec[] lprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

		};

		IndexType b_index = new IndexType(IndexType.Interval_Index);
		Iterator it = null;
		try {
			it = new IndexScan(b_index, HEAPFILENAME, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, null, 3,
					false);
		}

		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}
		Tuple t;
		t = null;
		try {
			while ((t = it.get_next()) != null) {
				t.print(ltypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println("\n");
		try {
			it.close();
		} catch (Exception e) {

			e.printStackTrace();
		}		
	}
	private void printBeforeIndex() {

		
		CondExpr[] leftFilter = new CondExpr[2];
		leftFilter[0] = new CondExpr();

		leftFilter[0].next = null;
		leftFilter[0].op = new AttrOperator(AttrOperator.aopLE);
		leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		leftFilter[0].type2 = new AttrType(AttrType.attrInterval);
		leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		leftFilter[0].operand2.intervaltype = new Intervaltype(-299973,Integer.MIN_VALUE);
		leftFilter[0].flag =1;

		leftFilter[1] = null;
		AttrType[] ltypes = new AttrType[3];
			ltypes[0] = new AttrType(AttrType.attrInterval);
			ltypes[1] = new AttrType(AttrType.attrInteger);
			ltypes[2] = new AttrType(AttrType.attrString);
		

		short[] lsizes = new short[1];
			lsizes[0] = 10;


		FldSpec[] lprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

		};

		IndexType b_index = new IndexType(IndexType.Interval_Index);
		Iterator it = null;
		try {
			it = new IndexScan(b_index, HEAPFILENAME, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
					false);
		}

		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}
		Tuple t;
		t = null;
		try {
			while ((t = it.get_next()) != null) {
				t.print(ltypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println("\n");
		try {
			it.close();
		} catch (Exception e) {

			e.printStackTrace();
		}		
	}
private void printAfterIndex() {

		
		CondExpr[] leftFilter = new CondExpr[2];
		leftFilter[0] = new CondExpr();

		leftFilter[0].next = null;
		leftFilter[0].op = new AttrOperator(AttrOperator.aopGE);
		leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		leftFilter[0].type2 = new AttrType(AttrType.attrInterval);
		leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		leftFilter[0].operand2.intervaltype = new Intervaltype(-299973,Integer.MAX_VALUE);
		leftFilter[0].flag =1;

		leftFilter[1] = null;
		AttrType[] ltypes = new AttrType[3];
			ltypes[0] = new AttrType(AttrType.attrInterval);
			ltypes[1] = new AttrType(AttrType.attrInteger);
			ltypes[2] = new AttrType(AttrType.attrString);
		

		short[] lsizes = new short[1];
			lsizes[0] = 10;


		FldSpec[] lprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

		};

		IndexType b_index = new IndexType(IndexType.Interval_Index);
		Iterator it = null;
		try {
			it = new IndexScan(b_index, HEAPFILENAME, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
					false);
		}

		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}
		Tuple t;
		t = null;
		try {
			while ((t = it.get_next()) != null) {
				t.print(ltypes);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println("\n");
		try {
			it.close();
		} catch (Exception e) {

			e.printStackTrace();
		}		
	}


private void printSpanIndex() {

	
	CondExpr[] leftFilter = new CondExpr[3];
	leftFilter[0] = new CondExpr();

	leftFilter[0].next = null;
	leftFilter[0].op = new AttrOperator(AttrOperator.aopLE);
	leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	leftFilter[0].type2 = new AttrType(AttrType.attrInterval);
	leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	leftFilter[0].operand2.intervaltype = new Intervaltype(-299967,Integer.MIN_VALUE);
	leftFilter[0].flag =1;

	leftFilter[1] = new CondExpr();
	leftFilter[1].next = null;
	leftFilter[1].op = new AttrOperator(AttrOperator.aopGE);
	leftFilter[1].type1 = new AttrType(AttrType.attrSymbol);
	leftFilter[1].type2 = new AttrType(AttrType.attrInterval);
	leftFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	leftFilter[1].operand2.intervaltype = new Intervaltype(-299973,Integer.MAX_VALUE);
	leftFilter[1].flag =1;
	
	leftFilter[2] = null;
	
	AttrType[] ltypes = new AttrType[3];
		ltypes[0] = new AttrType(AttrType.attrInterval);
		ltypes[1] = new AttrType(AttrType.attrInteger);
		ltypes[2] = new AttrType(AttrType.attrString);
	

	short[] lsizes = new short[1];
		lsizes[0] = 10;


	FldSpec[] lprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
			new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

	};

	IndexType b_index = new IndexType(IndexType.Interval_Index);
	Iterator it = null;
	try {
		it = new IndexScan(b_index, HEAPFILENAME, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
				false);
	}

	catch (Exception e) {
		System.err.println("*** Error creating scan for Index scan");
		System.err.println("" + e);
		Runtime.getRuntime().exit(1);
	}
	Tuple t;
	t = null;
	try {
		while ((t = it.get_next()) != null) {
			t.print(ltypes);
		}
	} catch (Exception e) {
		System.err.println("" + e);
		e.printStackTrace();
		Runtime.getRuntime().exit(1);
	}

	System.out.println("\n");
	try {
		it.close();
	} catch (Exception e) {

		e.printStackTrace();
	}		
}


	private void Disclaimer() {
		System.out.print("\n\nAny resemblance of persons in this database to"
				+ " people living or dead\nis purely coincidental. The contents of "
				+ "this database do not reflect\nthe views of the University,"
				+ " the Computer  Sciences Department or the\n" + "developers...\n\n");
	}

	public static void main(String argv[]) {
		boolean sortstatus = false;

		try {
		IntervalTIndexTest nlj = new IntervalTIndexTest(argv[0]);
		nlj.Disclaimer();
		nlj.createIndex();
		
		System.out.println("Hopefully created index");
		
		System.out.println("--------Iterating All Indexes----------");
		nlj.printAllIndex();
		System.out.println("--------Iterting 0- Before Indexes------");
		nlj.printBeforeIndex();
		System.out.println("--------Iterating 3- after Indexes-------");
		nlj.printAfterIndex();
		System.out.println("--------Iterating Span Indexes----------");
		nlj.printSpanIndex();
		sortstatus = true;
		System.out.println("Hopefully created index");
		
		}
		catch(Exception e){
			e.printStackTrace();
		}
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		} else {
			System.out.println("join tests completed successfully");
		}
	}
}
