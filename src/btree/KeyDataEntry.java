/*
 * @(#) bt.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *        Author Xiaohu Li (xiaohu@cs.wisc.edu)
 */
package btree;
import global.*;
import intervaltree.IntervalKey;
import intervaltree.TreeData;

/** KeyDataEntry: define (key, data) pair.
 */
public class KeyDataEntry {
   /** key in the (key, data)
    */  
   public KeyClass key;
   /** data in the (key, data)
    */
   public DataClass data;
   
  /** Class constructor
   */
  public KeyDataEntry( Integer key, PageId pageNo) {
     this.key = new IntegerKey(key); 
     this.data = new IndexData(pageNo);
  }; 

  public KeyDataEntry( Intervaltype key, PageId pageNo) {
	     this.key = new IntervalKey(key); 
	     this.data = new IndexData(pageNo);
	  }; 


  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, PageId pageNo) {

     data = new IndexData(pageNo); 
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey());   
     else if(key instanceof IntervalKey)
    	 this.key = new IntervalKey(((IntervalKey)key).getKey());
  };


  /** Class constructor.
   */
  public KeyDataEntry( String key, PageId pageNo) {
     this.key = new StringKey(key); 
     this.data = new IndexData(pageNo);
  };

  /** Class constructor.
   */
  public KeyDataEntry( Integer key, RID rid) {
     this.key = new IntegerKey(key); 
     this.data = new LeafData(rid);
  }

  public KeyDataEntry( Intervaltype key, RID rid) {
	     this.key = new IntervalKey(key); 
	     this.data = new TreeData(rid);
	  }
  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key, RID rid){
     if ( key instanceof IntegerKey ) {
    	 this.key= new IntegerKey(((IntegerKey)key).getKey()); 
    	 data = new LeafData(rid); 
     } 
     else if ( key instanceof StringKey ) {
    	 this.key= new StringKey(((StringKey)key).getKey()); 
    	 data = new LeafData(rid); 
     }
     else if(key instanceof IntervalKey) {
    	 this.key = new IntervalKey(((IntervalKey)key).getKey());
    	 data = new TreeData(rid);
     }
  };


  /** Class constructor.
   */
  public KeyDataEntry( String key, RID rid) {
     this.key = new StringKey(key); 
     this.data = new LeafData(rid);
  }; 

  /** Class constructor.
   */
  public KeyDataEntry( KeyClass key,  DataClass data) {
     if ( key instanceof IntegerKey ) 
        this.key= new IntegerKey(((IntegerKey)key).getKey());
     else if ( key instanceof StringKey ) 
        this.key= new StringKey(((StringKey)key).getKey()); 
     else if (key instanceof IntervalKey)
    	 this.key = new IntervalKey(((IntervalKey)key).getKey());

     if ( data instanceof IndexData ) 
        this.data= new IndexData(((IndexData)data).getData());
     else if ( data instanceof LeafData ) 
        this.data= new LeafData(((LeafData)data).getData()); 
     else if(data instanceof TreeData)
    	 this.data =  new TreeData(((TreeData) data).getData());
  }

  /** shallow equal. 
   *  @param entry the entry to check again key. 
   *  @return true, if entry == key; else, false.
   */
  public boolean equals(KeyDataEntry entry) {
      boolean st1,st2;

      if ( key instanceof IntegerKey )
         st1= ((IntegerKey)key).getKey().equals
                  (((IntegerKey)entry.key).getKey());
      else if (key instanceof IntervalKey)
    	  st1 = ((IntervalKey) this.key).getKey().equals(((IntervalKey) entry.key).getKey());
      else 
         st1= ((StringKey)key).getKey().equals
                  (((StringKey)entry.key).getKey());

      if( data instanceof IndexData )
         st2= ( (IndexData)data).getData().pid==
              ((IndexData)entry.data).getData().pid ;
      else if(data instanceof TreeData)
    	  st2 = ((RID)((TreeData)data).getData()).equals
                  (((RID)((TreeData)entry.data).getData()));
      else
         st2= ((RID)((LeafData)data).getData()).equals
                (((RID)((LeafData)entry.data).getData()));

  
      return (st1&&st2);
  }     
}

