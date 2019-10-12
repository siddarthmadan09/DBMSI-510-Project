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
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.NodeTuple;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.NestedLoopsJoinsIndexScan;
import iterator.RelSpec;

public class NLJIndexTest {

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

	public void QueryNLJwithIndex(String patternTreeFile) {

		System.out.print("**********************QueryNLJwithIndex starting *********************\n");

		String st;
		try {
			BufferedReader br = new BufferedReader(new FileReader(patternTreeFile));
			st = br.readLine();
			int nodesCount = Integer.valueOf(st);

			HashMap<Integer, String> map = new HashMap<Integer, String>();

			for (int i = 0; i < nodesCount; i++) {
				st = br.readLine();
				map.put(i + 1, st);
			}

			NestedLoopsJoins inl = null;
			List<String> conditions = new ArrayList<String>();
			while ((st = br.readLine()) != null) {
				conditions.add(st);
			}
			int dynamicCount = -1;
			HashMap<Integer, String> dynamic = new HashMap<Integer, String>();

			// --------------------------------------Query Plan
			// Index-----------------------------------------
			// Uncomment QueryPlanExecutorIndex() to run Query Plan index
			/*
			 * Query Plan that uses deep tree travsersal using Indexs B+ tree indexes are
			 * created on Nodename field We can use indexScan on this B+ tree indexs. And
			 * QueryIndex uses index
			 */

			System.out.println("Query plan :Index");
			QueryPlanExecutorIndex(map, conditions, inl, 0, HEAPFILENAME, dynamicCount, dynamic, INDEXNAME);
			System.out.println("for index:" + SystemDefs.JavabaseBM.pcounter);

		} catch (Exception e) {

		}
	}

	public NLJIndexTest(String XMLPath) {
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

	private void Disclaimer() {
		System.out.print("\n\nAny resemblance of persons in this database to"
				+ " people living or dead\nis purely coincidental. The contents of "
				+ "this database do not reflect\nthe views of the University,"
				+ " the Computer  Sciences Department or the\n" + "developers...\n\n");
	}

	public void QueryPlanExecutorIndex(HashMap<Integer, String> map, List<String> conditions, Iterator it,
			int conditionCount, String heapFileName, int dynamicCount, HashMap<Integer, String> dynamic,
			String IndexName) {

		if (conditionCount >= conditions.size()) {
			NestedLoopsJoinsIndexScan nlj = (NestedLoopsJoinsIndexScan) it;
			int sizeofTuple = nlj.getFinalTupleSize();

			AttrType[] outputtype = new AttrType[sizeofTuple];

			for (int i = 0; i < sizeofTuple; i = i + 3) {
				outputtype[i] = new AttrType(AttrType.attrInterval);
				outputtype[i + 1] = new AttrType(AttrType.attrInteger);
				outputtype[i + 2] = new AttrType(AttrType.attrString);

			}

			Tuple t;
			t = null;
			try {
				while ((t = nlj.get_next()) != null) {
					t.print(outputtype);
				}
			} catch (Exception e) {
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			System.out.println("\n");
			try {
				nlj.close();
			} catch (Exception e) {

				e.printStackTrace();
			}

			return;
		}

		// ------------
		String[] splited = conditions.get(conditionCount).split("\\s+");
		int index = 0;
		String notRepatedElement = null;
		if (!dynamic.containsValue(map.get(Integer.valueOf(splited[0])))) {
			dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[0])));
			notRepatedElement = map.get(Integer.valueOf(splited[0]));
		} else {
			for (Map.Entry<Integer, String> e : dynamic.entrySet()) {
				if (e.getValue().equals(map.get(Integer.valueOf(splited[0])))) {
					index = e.getKey();
					break;
				}
			}
		}

		if (!dynamic.containsValue(map.get(Integer.valueOf(splited[1])))) {
			dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[1])));
			notRepatedElement = map.get(Integer.valueOf(splited[1]));
		} else {
			for (Map.Entry<Integer, String> e : dynamic.entrySet()) {
				if (e.getValue().equals(map.get(Integer.valueOf(splited[1])))) {
					index = e.getKey();
					break;
				}
			}
		}

		// parsing for condition expressions
		CondExpr[] leftFilter = new CondExpr[2];
		leftFilter[0] = new CondExpr();

		leftFilter[0].next = null;
		leftFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		leftFilter[0].type2 = new AttrType(AttrType.attrString);
		leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
		leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));

		leftFilter[1] = null;

		CondExpr[] rightFilter = new CondExpr[2];
		rightFilter[0] = new CondExpr();

		rightFilter[0].next = null;
		rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		rightFilter[0].type2 = new AttrType(AttrType.attrString);
		rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);

		if (notRepatedElement != null)
			rightFilter[0].operand2.string = notRepatedElement;
		else
			rightFilter[0].operand2.string = map.get(Integer.valueOf(splited[1]));
		rightFilter[1] = null;

		String relationship = splited[2];

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		outFilter[0].next = null;
		outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), index * 3 + 1);
		outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		outFilter[0].flag = 1;
		// outFilter[1] = null;

		if (relationship.equals("PC")) {

			outFilter[1].next = null;
			outFilter[1].op = new AttrOperator(AttrOperator.aopLT);
			outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
			outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
			outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), index * 3 + 2);

			outFilter[1].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			outFilter[1].flag = 1;
			outFilter[2] = null;
		} else if (relationship.equals("AD")) {
			outFilter[1] = null;
			outFilter[2] = null;
		}

		AttrType[] ltypes = new AttrType[(conditionCount + 1) * 3];
		for (int j = 0; j < (conditionCount + 1) * 3; j = j + 3) {
			ltypes[j] = new AttrType(AttrType.attrInterval);
			ltypes[j + 1] = new AttrType(AttrType.attrInteger);
			ltypes[j + 2] = new AttrType(AttrType.attrString);
		}

		short[] lsizes = new short[(conditionCount + 1)];
		for (int j = 0; j < lsizes.length; j++)
			lsizes[j] = 10;

		AttrType[] rtypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), };

		short[] rsizes = new short[1];
		rsizes[0] = 10;

		if (it == null) {

			FldSpec[] lprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

			};

			IndexType b_index = new IndexType(IndexType.B_Index);
			try {
				it = new IndexScan(b_index, heapFileName, IndexName, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
						false);
			}

			catch (Exception e) {
				System.err.println("*** Error creating scan for Index scan");
				System.err.println("" + e);
				Runtime.getRuntime().exit(1);
			}

		}
		// from 2nd condition---

		int fieldCounts = (conditionCount + 2) * 3;
		FldSpec[] proj1 = new FldSpec[fieldCounts];

		// for outer relations
		for (int i = 0; i < fieldCounts - 3; i = i + 3) {
			proj1[i] = new FldSpec(new RelSpec(RelSpec.outer), 1 + i);
			proj1[i + 1] = new FldSpec(new RelSpec(RelSpec.outer), 2 + i);
			proj1[i + 2] = new FldSpec(new RelSpec(RelSpec.outer), 3 + i);
		}

		// for inner relations
		proj1[fieldCounts - 3] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		proj1[fieldCounts - 2] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		proj1[fieldCounts - 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

		FldSpec[] Indexprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

		};

		NestedLoopsJoinsIndexScan inl = null;
		try {
			inl = new NestedLoopsJoinsIndexScan(ltypes, ltypes.length, lsizes, rtypes, 3, rsizes, 10, it, heapFileName,
					outFilter, rightFilter, proj1, fieldCounts, INDEXNAME, 3, 3, Indexprojection, 3, false);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		QueryPlanExecutorIndex(map, conditions, inl, conditionCount + 1, heapFileName, dynamicCount, dynamic,
				IndexName);

	}

	public static void main(String argv[]) {
		boolean sortstatus = false;

		try {
		NLJIndexTest nlj = new NLJIndexTest(argv[0]);
		nlj.Disclaimer();
		nlj.createIndex();
		nlj.QueryNLJwithIndex(argv[1]);
		sortstatus = true;
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
