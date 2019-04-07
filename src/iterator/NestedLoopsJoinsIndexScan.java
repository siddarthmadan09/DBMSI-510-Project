package iterator;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;

/**
 *
 * This file contains an implementation of the nested loops join algorithm as
 * described in the Shapiro paper. The algorithm is extremely simple:
 *
 * foreach tuple r in R do foreach tuple s in S do if (ri == sj) then add (r, s)
 * to the result.
 */

public class NestedLoopsJoinsIndexScan extends Iterator {
	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator outer;
	private short t2_str_sizescopy[];
	private CondExpr OutputFilter[];
	private CondExpr RightFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private boolean done, // Is the join complete
			get_from_outer; // if TRUE, a tuple is got from outer
	private Tuple outer_tuple, inner_tuple;
	private Tuple Jtuple; // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;
	// private Heapfile hf;

	// Changes for index

	private IndexScan inner;
	private String IndexName;
	private int IndexNoInFlds;
	private int IndexNoOutFlds;
	private FldSpec IndexOutFlds[];
	private int IndexFldNum;
	private boolean IndexOnly;
	private String RelationName;

	public int getFinalTupleSize() {
		return perm_mat.length;
	}

	/**
	 * constructor Initialize the two relations which are joined, including relation
	 * type, It assumes that B+ Tree index is already created, create B+ tree Index
	 * before using this
	 * 
	 * @param in1            Array containing field types of R.
	 * @param len_in1        # of columns in R.
	 * @param t1_str_sizes   shows the length of the string fields.
	 * @param in2            Array containing field types of S
	 * @param len_in2        # of columns in S
	 * @param t2_str_sizes   shows the length of the string fields.
	 * @param amt_of_mem     IN PAGES
	 * @param am1            access method for left i/p to join
	 * @param relationName   access hfapfile for right i/p to join
	 * @param outFilter      select expressions
	 * @param rightFilter    reference to filter applied on right i/p
	 * @param proj_list      shows what input fields go where in the output tuple
	 * @param n_out_flds     number of outer relation fields
	 * @param indexName      Name of the Index, Not the name of the heapfile
	 * @param indexnoInFlds  No of fields to select from Index, always 3 for normal
	 *                       tuple of (attrinterval, level,nodename)
	 * @param indexnoOutFlds No of fields to select fir result, can be 1 for index
	 *                       only(nodename), can be 2 (attrInterval, Nodename), can
	 *                       be 3(attrInterval, level, nodename)
	 * @param indexoutFlds   Field specification for result, can be 1 for indexOnly
	 *                       (FieldSpec.String), Can be 2 for (FldSpec.attrInterval,
	 *                       FldSpec.String), Can be 3 for (FldSpec.AttrInterval,
	 *                       FldSpc.Integer, FldSpec.String)
	 * @param indexfldNum    column number on which condition should be applied (1 -
	 *                       AttrInterval, 2 - Level, 3 - nodename)
	 * @param indexOnly      Boolean indicating if index created should be indexOnly
	 *                       or not, True - indexonly, False - Select whole tuple,
	 *                       SHould be False in most of the casez
	 * @exception IOException         some I/O fault
	 * @exception NestedLoopException exception from this class
	 */
	public NestedLoopsJoinsIndexScan(AttrType in1[], 
			int len_in1, 
			short t1_str_sizes[], 
			AttrType in2[], // This will be used for index
			int len_in2, // This will be used for index, inner
			short t2_str_sizes[], // Inner index
			int amt_of_mem, 
			Iterator am1, 
			String relationName, // Name of the heapfile
			CondExpr outFilter[], 
			CondExpr rightFilter[], // Condition for Index
			FldSpec proj_list[], 
			int n_out_flds, 
			String indexName, // Name of the index
			int indexnoInFlds, // This is for setting number of fields in inner index // can be 2 for indexOnly
								// where it will select only index key and RID and can be 3 when it will select
								// whole tuple
			int indexnoOutFlds, // Projection list in index
			FldSpec indexoutFlds[], // Fields in projection list for Index
			int indexfldNum, // which field /column to select from index
			final boolean indexOnly // True - Indexonly, False- others

	) throws IOException, NestedLoopException {

		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length]; // Attribute Types for Index
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2; // length for Index

		outer = am1;
		t2_str_sizescopy = t2_str_sizes; // string size for index
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter = rightFilter; // Filter for Index

		n_buf_pgs = amt_of_mem;
		inner = null;
		done = false;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;

		// Setting properties related to Index
		IndexName = indexName;
		IndexNoInFlds = indexnoInFlds;
		IndexNoOutFlds = indexnoOutFlds;
		IndexOutFlds = indexoutFlds;
		IndexFldNum = indexfldNum;
		IndexOnly = indexOnly;
		RelationName = relationName;

		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes,
					proj_list, nOutFlds);
		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
		}

		/*
		 * try { btree = new BTreeFile(IndexName); } catch (GetFileEntryException |
		 * PinPageException | ConstructPageException e) { throw new
		 * NestedLoopException(e, "Create new heapfile failed."); }
		 */

		/*
		 * try { hf = new Heapfile(relationName);
		 * 
		 * } catch(Exception e) { throw new NestedLoopException(e,
		 * "Create new heapfile failed."); }1 for indexOnly and
		 */
	}

	/**
	 * @return The joined tuple is returned
	 * @exception IOException               I/O errors
	 * @exception JoinsException            some join exception
	 * @exception IndexException            exception from super class
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception InvalidTypeException      tuple type not valid
	 * @exception PageNotReadException      exception from lower layer
	 * @exception TupleUtilsException       exception from using tuple utilities
	 * @exception PredEvalException         exception from PredEval class
	 * @exception SortException             sort exception
	 * @exception LowMemException           memory error
	 * @exception UnknowAttrType            attribute type unknown
	 * @exception UnknownKeyTypeException   key type unknown
	 * @exception Exception                 other exceptions
	 * 
	 */
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// This is a DUMBEST form of a join, not making use of any key information...

		if (done)
			return null;

		do {
			// If get_from_outer is true, Get a tuple from the outer, delete
			// an existing scan on the file, and reopen a new scan on the file.
			// If a get_next on the outer returns DONE?, then the nested loops
			// join is done too.

			if (get_from_outer == true) {
				get_from_outer = false;
				if (inner != null) // If this not the first time,
				{
					// close scan
					inner = null;
				}

				// In normal NestedLoopjoin, FileScan is opened here for inner relation but now
				// it will use IndexScan for inner relation
				try {
					inner = new IndexScan(new IndexType(IndexType.B_Index), RelationName, IndexName, _in2,
							t2_str_sizescopy, IndexNoInFlds, IndexNoOutFlds, IndexOutFlds, RightFilter, IndexFldNum,
							false);
				} catch (Exception e) {
					throw new NestedLoopException(e, "IndexScan failed");
				}

				if ((outer_tuple = outer.get_next()) == null) {
					done = true;
					if (inner != null) {

						inner = null;
					}

					return null;
				}
			} // ENDS: if (get_from_outer == TRUE)

			// The next step is to get a tuple from the inner,
			// while the inner is not completely scanned && there
			// is no match (with pred),get a tuple from the inner.

			while ((inner_tuple = inner.get_next()) != null) {
				inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
				if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true) {
					if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true) {
						// Apply a projection on the outer and inner tuples.
						Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
						return Jtuple;
					}
				}
			}

			// There has been no match. (otherwise, we would have
			// returned from t//he while loop. Hence, inner is
			// exhausted, => set get_from_outer = TRUE, go to top of loop

			get_from_outer = true; // Loop back to top and get next outer tuple.
		} while (true);
	}

	/**
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 * 
	 * @exception IOException    I/O error from lower layers
	 * @exception JoinsException join error from lower layers
	 * @exception IndexException index access error
	 */
	public void close() throws JoinsException, IOException, IndexException {
		if (!closeFlag) {

			try {
				outer.close();
			} catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}
