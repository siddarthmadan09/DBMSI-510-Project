package iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import global.AttrOperator;
import global.AttrType;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;

public class QueryPlanExecutor {
    public AttrType []  outputtype;
	private HashMap<Integer,String> dynamic;
    
    
    public QueryPlanExecutor() {
        this.dynamic = new HashMap<Integer,String>();
    }
    
    
	public Iterator  QueryPlanExecutor1(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount, String heapFileName,int dynamicCount) {
	   	
		  if(conditionCount >= conditions.size()) {
//		      NestedLoopsJoins nlj = (NestedLoopsJoins)it;
//              int sizeofTuple = nlj.getFinalTupleSize();
////              
//              this.outputtype = new AttrType[sizeofTuple];
////                
//              for(int i=0;i< sizeofTuple;i=i+3) {
//                  outputtype[i]= new AttrType(AttrType.attrInterval);
//                  outputtype[i+1]=new AttrType(AttrType.attrInteger);
//                  outputtype[i+2]=new AttrType(AttrType.attrString);
//                    
//                }
////            
//              Tuple t;
//                t = null;
//                try {
//                  while ((t = nlj.get_next()) != null) {
//                    t.print(outputtype);
//                  
//                  }
//                }
//                catch (Exception e) {
//                  System.err.println (""+e);
//                  e.printStackTrace();
//                  Runtime.getRuntime().exit(1);
//                }
//
//                System.out.println ("\n"); 
//                try {
//                  nlj.close();
//                }
//                catch (Exception e) {
//                
//                  e.printStackTrace();
//                }
		      
		      return it;
			    
			  
//			  return ;
		  }
		  
	//------------	  
		  String[] splited=conditions.get(conditionCount).split("\\s+");
		  
		 // if(dynamic.get(map.get(key)))
		  int index=0;
		  if(!this.dynamic.containsValue(map.get(Integer.valueOf(splited[0])))){
			  this.dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[0])));
		  }else {
			  for(Map.Entry<Integer,String> e : this.dynamic.entrySet()) {
				  if(e.getValue().equals(map.get(Integer.valueOf(splited[0]))))
				  	index = e.getKey();
			  }
		  }	  
			  
		  if(!this.dynamic.containsValue(map.get(Integer.valueOf(splited[1])))){
				  this.dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[1])));
		  }else {
					  for(Map.Entry<Integer,String> e : this.dynamic.entrySet()) {
						  if(e.getValue().equals(map.get(Integer.valueOf(splited[1]))))
						  	index = e.getKey();
					  }
				  }	  
					
		  //parsing for condition expressions
		  CondExpr [] leftFilter  = new CondExpr[2];
	      leftFilter[0] = new CondExpr();
	      
	      leftFilter[0].next  = null;
	      leftFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
	      leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	      leftFilter[0].type2 = new AttrType(AttrType.attrString);
	      leftFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
	      leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));
	      
	      leftFilter[1] = null;
	      
	      CondExpr [] rightFilter = new CondExpr[2];
	      rightFilter[0] = new CondExpr();
	      
	      rightFilter[0].next  = null;
	      rightFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
	      rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	      rightFilter[0].type2 = new AttrType(AttrType.attrString);
	      rightFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
	      rightFilter[0].operand2.string = map.get(Integer.valueOf(splited[1]));
	      rightFilter[1] = null;

	      String relationship= splited[2];
	      
	      CondExpr [] outFilter = new CondExpr[3];
	      outFilter[0] = new CondExpr();
	      outFilter[1] = new CondExpr();
	      
	      
	      outFilter[0].next  = null;
	      outFilter[0].op    = new AttrOperator(AttrOperator.aopGT);
	      outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
	      outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
	      outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),index*3+1);
	      outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
	      outFilter[0].flag=1;
	     // outFilter[1] = null;

	      if(relationship.equals("PC")) {
	      	      
			      outFilter[1].next  = null;
			      outFilter[1].op    = new AttrOperator(AttrOperator.aopLT);
			      outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
			      outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
			      outFilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),index*3+2);
		
			      outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
			      outFilter[1].flag=1;
			      outFilter[2] = null;
	      }
	      else if(relationship.equals("AD"))
	      {	    	  
	    	  outFilter[1] = null;
	    	  outFilter[2] = null;					       
	      }

	      AttrType [] ltypes = new AttrType[(conditionCount+1)*3];
	      for(int j=0;j<(conditionCount+1)*3;j=j+3) {
		ltypes[j] = new AttrType(AttrType.attrInterval);
		ltypes[j+1]=new AttrType(AttrType.attrInteger); 
		ltypes[j+2]=new AttrType(AttrType.attrString);
	      }
	    
	      short []   lsizes = new short[(conditionCount+1)];
	      for(int j=0;j<lsizes.length;j++)
	    	  lsizes[j]=10;
	         
	      AttrType [] rtypes = {
		new AttrType(AttrType.attrInterval), 
		new AttrType(AttrType.attrInteger), 
		new AttrType(AttrType.attrString), 
	      };
	      
	      short  []  rsizes = new short[1] ;
	      rsizes[0] = 10;
	    
	      if(it==null) {
	    	  
	    	  FldSpec [] lprojection = {
	  				new FldSpec(new RelSpec(RelSpec.outer), 1),
	  				new FldSpec(new RelSpec(RelSpec.outer), 2),
	  			    new FldSpec(new RelSpec(RelSpec.outer), 3),

	  			      };

			      boolean status=true;
				try {
				it  = new FileScan(heapFileName, ltypes, lsizes, 
						   (short)3, (short)3,
						   lprojection, leftFilter);
			      }
			      catch (Exception e) {
				status = false;
				System.err.println (""+e);
				e.printStackTrace();
			      }
			      
			      if (status != true) {
				//bail out
				
				System.err.println ("*** Error setting up scan for sailors");
				Runtime.getRuntime().exit(1);
			      }
			
	      }
	      //from 2nd condition---
			  
	    	  int fieldCounts = (conditionCount +2)*3;
	    	  FldSpec []  proj1 = new FldSpec[fieldCounts];
	    		
	    	  //for outer relations
	    	  for(int i=0;i< fieldCounts-3;i=i+3) {
	    		  proj1[i]=new FldSpec(new RelSpec(RelSpec.outer), 1+i);
	    		  proj1[i+1]=new FldSpec(new RelSpec(RelSpec.outer), 2+i);
	    		  proj1[i+2]=new FldSpec(new RelSpec(RelSpec.outer), 3+i);
	 	     }
	   	  
	    	  //for inner relations
	    	  proj1[fieldCounts-3]=new FldSpec(new RelSpec(RelSpec.innerRel), 1);
			  proj1[fieldCounts-2]=new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			  proj1[fieldCounts-1]=new FldSpec(new RelSpec(RelSpec.innerRel), 3);
		    	
			   NestedLoopsJoins inl = null;
			      try {
				inl = new NestedLoopsJoins (ltypes, ltypes.length, lsizes,
							    rtypes, 3, rsizes,
							    10,
							  it, heapFileName,
							    outFilter, rightFilter, proj1, fieldCounts);
			      }
			      catch (Exception e) {
				System.err.println ("*** Error preparing for nested_loop_join");
				System.err.println (""+e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			      }
			
			      return QueryPlanExecutor1(map, conditions, inl, conditionCount+1, heapFileName, dynamicCount);
//			      System.out.println("Continuing query 1");
			      
	    	 	  
	  }

	
	public void  QueryPlanExecutor2(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
		
	}
	public HashMap<Integer, String> getDynamic() {
        return dynamic;
    }


    public void setDynamic(HashMap<Integer, String> dynamic) {
        this.dynamic = dynamic;
    }


    public void  QueryPlanExecutor3(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
	}
	
}
