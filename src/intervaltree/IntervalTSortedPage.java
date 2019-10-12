/*
 * @(#) SortedPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *
 *      by Xiaohu Li (xiaohu@cs.wisc.edu)
 */

package intervaltree;

import java.io.IOException;

import btree.ConstructPageException;
import btree.DeleteRecException;
import btree.InsertRecException;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.NodeType;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

/**
 * BTsortedPage class just holds abstract records in sorted order, based on how
 * they compare using the key interface from BT.java.
 */
public class IntervalTSortedPage extends HFPage {

	int keyType; // it will be initialized in IntervalTFile

	/**
	 * pin the page with pageno, and get the corresponding SortedPage
	 * 
	 * @param pageno  input parameter. To specify which page number the IntervalTSortedPage
	 *                will correspond to.
	 * @param keyType input parameter. It specifies the type of key. It can be
	 *                AttrType.attrInterval only.
	 * @exception ConstructPageException error for IntervalTSortedPage constructor
	 */
	public IntervalTSortedPage(PageId pageno, int keyType) throws ConstructPageException {
		super();
		try {
			// super();
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
			this.keyType = keyType;
		} catch (Exception e) {
			throw new ConstructPageException(e, "construct sorted page failed");
		}
	}

	/**
	 * associate the SortedPage instance with the Page instance
	 * 
	 * @param page    input parameter. To specify which page the IntervalTSortedPage will
	 *                correspond to.
	 * @param keyType input parameter. It specifies the type of key. It can be
	 *                AttrType.attrInterval only.
	 */
	public IntervalTSortedPage(Page page, int keyType) {

		super(page);
		this.keyType = keyType;
	}

	/**
	 * new a page, and associate the SortedPage instance with the Page instance
	 * 
	 * @param keyType input parameter. It specifies the type of key. It can be
	 *                AttrType.attrInterval
	 * @exception ConstructPageException error for IntervalTSortedPage constructor
	 */
	public IntervalTSortedPage(int keyType) throws ConstructPageException {
		super();
		try {
			Page apage = new Page();
			PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
			if (pageId == null)
				throw new ConstructPageException(null, "construct new page failed");
			this.init(pageId, apage);
			this.keyType = keyType;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ConstructPageException(e, "construct sorted page failed");
		}
	}

	/**
	 * Performs a sorted insertion of a record on an record page. The records are
	 * sorted in increasing key order. Only the slot directory is rearranged. The
	 * data records remain in the same positions on the page.
	 * 
	 * @param entry the entry to be inserted. Input parameter.
	 * @return its rid where the entry was inserted; null if no space left.
	 * @exception InsertRecException error when insert
	 */
	protected RID insertRecord(KeyDataEntry entry) throws InsertRecException {
		int i;
		short nType;
		RID rid;
		byte[] record;
		// ASSERTIONS:
		// - the slot directory is compressed; Inserts will occur at the end
		// - slotCnt gives the number of slots used

		// general plan:
		// 1. Insert the record into the page,
		// which is then not necessarily any more sorted
		// 2. Sort the page by rearranging the slots (insertion sort)

		try {

			record = IntervalT.getBytesFromEntry(entry);
			rid = super.insertRecord(record);
			if (rid == null)
				return null;

			if (entry.data instanceof TreeData)
				nType = NodeType.NODE;
			else // entry.data instanceof IndexData
				nType = NodeType.INDEX;

			// performs a simple insertion sort
			for (i = getSlotCnt() - 1; i > 0; i--) {

				KeyClass key_i, key_iplus1;

				key_i = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i), getSlotLength(i), keyType, nType).key;

				key_iplus1 = IntervalT.getEntryFromBytes(getpage(), getSlotOffset(i - 1), getSlotLength(i - 1), keyType,
						nType).key;

				if (IntervalT.keyCompare(key_i, key_iplus1) < 0) {
					// switch slots:
					int ln, off;
					ln = getSlotLength(i);
					off = getSlotOffset(i);
					setSlot(i, getSlotLength(i - 1), getSlotOffset(i - 1));
					setSlot(i - 1, ln, off);
				} else {
					// end insertion sort
					break;
				}

			}

			// ASSERTIONS:
			// - record keys increase with increasing slot number
			// (starting at slot 0)
			// - slot directory compacted

			rid.slotNo = i;
			return rid;
		} catch (Exception e) {
			throw new InsertRecException(e, "insert record failed");
		}

	} // end of insertRecord

	/**
	 * Deletes a record from a sorted record page. It also calls
	 * HFPage.compact_slot_dir() to compact the slot directory.
	 * 
	 * @param rid it specifies where a record will be deleted
	 * @return true if success; false if rid is invalid(no record in the rid).
	 * @exception DeleteRecException error when delete
	 */
	public boolean deleteSortedRecord(RID rid) throws DeleteRecException {
		try {

			deleteRecord(rid);
			compact_slot_dir();
			return true;
			// ASSERTIONS:
			// - slot directory is compacted
		} catch (Exception e) {
			if (e instanceof InvalidSlotNumberException)
				return false;
			else
				throw new DeleteRecException(e, "delete record failed");
		}
	} // end of deleteSortedRecord

	/**
	 * How many records are in the page
	 * 
	 * @param return the number of records.
	 * @exception IOException I/O errors
	 */
	protected int numberOfRecords() throws IOException {
		return getSlotCnt();
	}
};
