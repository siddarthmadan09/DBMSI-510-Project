package iterator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.TupleOrder;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;

public class QueryPlanExecutor {

    public static final String INDEXNAME = "BTreeIndexForNLJ";
    private HashMap<Integer,String> dynamic;
    
    
    public QueryPlanExecutor() {
        this.dynamic = new HashMap<Integer,String>();
    }

    public NestedLoopsJoinsIndexScan  QueryPlanExecutor1(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
        
        NestedLoopsJoinsIndexScan nlj = null;
//        
          //printing logic
          if(conditionCount >= conditions.size()) {
              nlj = (NestedLoopsJoinsIndexScan)it;
              int sizeofTuple = nlj.getFinalTupleSize();
              
              AttrType []  outputtype = new AttrType[sizeofTuple];
                
              for(int i=0;i< sizeofTuple;i=i+3) {
                  outputtype[i]= new AttrType(AttrType.attrInterval);
                  outputtype[i+1]=new AttrType(AttrType.attrInteger);
                  outputtype[i+2]=new AttrType(AttrType.attrString);
                    
                }
            
              Tuple t;
                t = null;
                try {
                  while ((t = nlj.get_next()) != null) {
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
                  nlj.close();
                }
                catch (Exception e) {
                
                  e.printStackTrace();
                }
                
              
              return nlj;
          }
          
          //set the dynamic map for output tuple to maintain indexes of output tuple  
          //dynamic count starts from 0 
          //for condition 1 2 AD 
          //              2 3 AD
          //-- for 1
          //nonreapeated element -- unique element in the condition
          
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

            //for 2
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
            //leftfilter for the first node tag value matching
            CondExpr[] leftFilter = new CondExpr[2];
            leftFilter[0] = new CondExpr();
            
            if (map.get(Integer.parseInt(splited[1])).contains("*"))
            {
                leftFilter=null;
            }
            else {
                
                leftFilter[0] = new CondExpr();
    
                leftFilter[0].next = null;
                leftFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                leftFilter[0].type2 = new AttrType(AttrType.attrString);
                leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
                leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));
    
                leftFilter[1] = null;
            
            }

            //for 2nd node - nonrepeated node 
            CondExpr[] rightFilter = new CondExpr[2];
            rightFilter[0] = new CondExpr();
            if (map.get(Integer.parseInt(splited[1])).contains("*"))
            {
                   rightFilter=null;
            }
            else {
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
    
            }


            String relationship = splited[2];

            //outfilter is to match the containtment between 2 nodes
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
                  IndexType b_index = new IndexType(IndexType.B_Index);
                    try {
                        it = new IndexScan(b_index, heapFileName, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
                                false);
                    }

                    catch (Exception e) {
                        System.err.println("*** Error creating scan for Index scan");
                        System.err.println("" + e);
                        Runtime.getRuntime().exit(1);
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
              
              FldSpec[] Indexprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
                        new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

                };

                
               NestedLoopsJoinsIndexScan inl = null;
                  try {
                 inl = new NestedLoopsJoinsIndexScan(ltypes, ltypes.length, lsizes, rtypes, 3, rsizes, 10, it, heapFileName,
                        outFilter, rightFilter, proj1, fieldCounts, INDEXNAME, 3, 3, Indexprojection, 3, false);
                  }
                  catch (Exception e) {
                System.err.println ("*** Error preparing for nested_loop_join");
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
                  }
            
                  QueryPlanExecutor1(map, conditions, inl, conditionCount+1, heapFileName, dynamicCount, dynamic);
                  
                  return inl;
      }

    public NestedLoopsJoinsIndexScan  QueryPlanExecutor1_iterative(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount_noneed, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
          
        NestedLoopsJoinsIndexScan inl = null;
           
              //set the dynamic map for output tuple to maintain indexes of output tuple  
          //dynamic count starts from 0 
          //for condition 1 2 AD 
          //              2 3 AD
          //-- for 1
          //nonreapeated element -- unique element in the condition
          
          for(int conditionCount=0;conditionCount<conditions.size();conditionCount++){
          
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
        
                    //for 2
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
                    //leftfilter for the first node tag value matching
                    CondExpr[] leftFilter = new CondExpr[2];
                    leftFilter[0] = new CondExpr();
        
                    if (map.get(Integer.parseInt(splited[1])).contains("*"))
                    {
                        leftFilter=null;
                    }
                    else {
                        
                        leftFilter[0] = new CondExpr();
            
                        leftFilter[0].next = null;
                        leftFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                        leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                        leftFilter[0].type2 = new AttrType(AttrType.attrString);
                        leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
                        leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));
            
                        leftFilter[1] = null;
                    
                    }

                    //for 2nd node - nonrepeated node 
                    CondExpr[] rightFilter = new CondExpr[2];
                    rightFilter[0] = new CondExpr();
                    if (map.get(Integer.parseInt(splited[1])).contains("*"))
                    {
                           rightFilter=null;
                    }
                    else {
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
            
                    }


                    String relationship = splited[2];
        
                    //outfilter is to match the containtment between 2 nodes
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
                      IndexType b_index = new IndexType(IndexType.B_Index);
                        try {
                            it = new IndexScan(b_index, heapFileName, INDEXNAME, ltypes, lsizes, 3, 3, lprojection, leftFilter, 3,
                                    false);
                        }

                        catch (Exception e) {
                            System.err.println("*** Error creating scan for Index scan");
                            System.err.println("" + e);
                            Runtime.getRuntime().exit(1);
                        }
          
                      if (status != true) {
                    //bail out
                    
                    System.err.println ("*** Error setting up scan for sailors");
                    Runtime.getRuntime().exit(1);
                      }

                    
                  }else{
                      it=inl;
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
                        
                      FldSpec[] Indexprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
                                new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),

                        };

                          
                              try {
                             inl = new NestedLoopsJoinsIndexScan(ltypes, ltypes.length, lsizes, rtypes, 3, rsizes, 10, it, heapFileName,
                                    outFilter, rightFilter, proj1, fieldCounts, INDEXNAME, 3, 3, Indexprojection, 3, false);
                              }
                              catch (Exception e) {
                            System.err.println ("*** Error preparing for nested_loop_join");
                            System.err.println (""+e);
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                              }
                    
                       //   QueryPlanExecutor1(map, conditions, inl, conditionCount+1, heapFileName, dynamicCount, dynamic);
          }         
          
                  return (NestedLoopsJoinsIndexScan)inl;
      }

    
    public void  QueryPlanExecutor2(HashMap<Integer,String > map, List<String> conditions,Iterator sm ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
          TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
          if(conditionCount >= conditions.size()) {
              SortMerge sm_final = (SortMerge)sm;
              int sizeofTuple = sm_final.getFinalTupleSize();
              
              AttrType []  outputtype = new AttrType[sizeofTuple];
                
              for(int i=0;i< sizeofTuple;i=i+3) {
                  outputtype[i]= new AttrType(AttrType.attrInterval);
                  outputtype[i+1]=new AttrType(AttrType.attrInteger);
                  outputtype[i+2]=new AttrType(AttrType.attrString);
                    
                }
            
              Tuple t;
                t = null;
                try {
                  while ((t = sm_final.get_next()) != null) {
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
                  sm.close();
                }
                catch (Exception e) {
                
                  e.printStackTrace();
                }
                
              
              return ;
          }
          
    //------------    
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
            if (map.get(Integer.parseInt(splited[1])).contains("*"))
            {
                leftFilter=null;
            }
            else {
                
                leftFilter[0] = new CondExpr();
    
                leftFilter[0].next = null;
                leftFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                leftFilter[0].type2 = new AttrType(AttrType.attrString);
                leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
                leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));
    
                leftFilter[1] = null;
            
            }

            CondExpr[] rightFilter = new CondExpr[2];
            if (map.get(Integer.parseInt(splited[1])).contains("*"))
            {
                   rightFilter=null;
            }
            else {
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
    
            }

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
        
          
          //for the first condition
          if(sm==null) {
              
              FldSpec [] lprojection = {
                    new FldSpec(new RelSpec(RelSpec.outer), 1),
                    new FldSpec(new RelSpec(RelSpec.outer), 2),
                    new FldSpec(new RelSpec(RelSpec.outer), 3),

                      };

                  boolean status=true;
                try {
                sm  = new FileScan("xml.in", ltypes, lsizes, 
                           (short)3, (short)3,
                           lprojection, leftFilter);
                
                Tuple sm_temp = null;
                System.out.println();
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
            int n_out_fld=0;
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
                
              // SortMerge sm = null;
               FileScan am2 = null;
                try {
                    am2  = new FileScan("xml.in", rtypes, rsizes, 
                            (short)3, (short)3,
                            proj1, rightFilter);
                }
                catch (Exception e) {
                    //status = FAIL;
                    System.err.println (""+e);
                    e.printStackTrace();
                }
                  try {
                      
                      n_out_fld = (conditionCount +2)*3;
                      
                      int joinColumn= index * 3 + 1;
                      //System.out.println(joinColumn);
                      sm = new SortMerge(ltypes, ltypes.length, lsizes,
                                rtypes, 3, rsizes,
                                joinColumn, 8, 
                                1, 8, 
                                (conditionCount+1)*10,
                                sm, am2, 
                                false, false, ascending,
                                outFilter, proj1, n_out_fld);                     }
                  catch (Exception e) {
                System.err.println ("*** Error preparing for sm_join");
                System.err.println (""+e);
                e.printStackTrace();
                Runtime.getRuntime().exit(1);
                  }
            
                  QueryPlanExecutor2(map, conditions, sm, conditionCount+1, heapFileName, dynamicCount, dynamic);             

    }
    
    public SortMerge QueryPlanExecutor2_iterative(HashMap<Integer,String > map, List<String> conditions,Iterator sm ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
        
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge sm_final = null; 
          
          for(conditionCount=0;conditionCount<conditions.size();conditionCount++){
                
                  String[] splited = conditions.get(conditionCount).split("\\s+");
                    int index = 0;
                    String notRepatedElement = null;
                    if (!dynamic.containsValue(map.get(Integer.valueOf(splited[0]))) ||  map.get(Integer.valueOf(splited[0])).equals("*")) {
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
        
                    if (!dynamic.containsValue(map.get(Integer.valueOf(splited[1])))||  map.get(Integer.valueOf(splited[1])).equals("*")) {
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
                    
                    if (map.get(Integer.parseInt(splited[1])).contains("*"))
                    {
                        leftFilter=null;
                    }
                    else {
                        
                        leftFilter[0] = new CondExpr();
            
                        leftFilter[0].next = null;
                        leftFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
                        leftFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                        leftFilter[0].type2 = new AttrType(AttrType.attrString);
                        leftFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
                        leftFilter[0].operand2.string = map.get(Integer.valueOf(splited[0]));
            
                        leftFilter[1] = null;
                    
                    }

                    //for 2nd node - nonrepeated node 
                    CondExpr[] rightFilter = new CondExpr[2];
                    rightFilter[0] = new CondExpr();
                    if (map.get(Integer.parseInt(splited[1])).contains("*"))
                    {
                           rightFilter=null;
                    }
                    else {
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
            
                    }

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
                
                  
                  //for the first condition
                  if(sm==null) {
                      
                      FldSpec [] lprojection = {
                            new FldSpec(new RelSpec(RelSpec.outer), 1),
                            new FldSpec(new RelSpec(RelSpec.outer), 2),
                            new FldSpec(new RelSpec(RelSpec.outer), 3),
        
                              };
        
                          boolean status=true;
                        try {
                        sm  = new FileScan("xml.in", ltypes, lsizes, 
                                   (short)3, (short)3,
                                   lprojection, leftFilter);
                        
                        /*
                         am  = new FileScan("xml.in", Stypes, Ssizes, 
                            (short)3, (short)3,
                            Sprojection, leftFilter);
                         */
                        Tuple sm_temp = null;
                        System.out.println();
                    /*  while( (sm_temp = sm.get_next()) != null) {
                            
                            System.out.println(sm_temp.getStrFld(3));
                        }*/
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
                    int n_out_fld=0;
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
                        
                      // SortMerge sm = null;
                       FileScan am2 = null;
                        try {
                            am2  = new FileScan("xml.in", rtypes, rsizes, 
                                    (short)3, (short)3,
                                    proj1, rightFilter);
                        }
                        catch (Exception e) {
                            //status = FAIL;
                            System.err.println (""+e);
                            e.printStackTrace();
                        }
                          try {
                              
                              n_out_fld = (conditionCount +2)*3;
                              
                              //System.out.println("recursion");
                            //  int joinColumn=((Integer.valueOf(splited[0]))-1)*3+1;
                              int joinColumn= index * 3 + 1;
                              //System.out.println(joinColumn);
                              sm = new SortMerge(ltypes, ltypes.length, lsizes,
                                        rtypes, 3, rsizes,
                                        joinColumn, 8, 
                                        1, 8, 
                                        (conditionCount+1)*20,
                                        sm, am2, 
                                        false, false, ascending,
                                        outFilter, proj1, n_out_fld);                     }
                          catch (Exception e) {
                        System.err.println ("*** Error preparing for sm_join");
                        System.err.println (""+e);
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                          }
                    
                       //   QueryPlanExecutor2(map, conditions, sm, conditionCount+1, heapFileName, dynamicCount, dynamic);    
                          
          }
          return (SortMerge)sm;
    }
    
    
    
    public SortMerge QueryPlanExecutor2_bushy(HashMap<Integer,String > map, List<String> conditions,Iterator sm1 ,Iterator sm2,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic ) {
        
        TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
        SortMerge sm_final = null; 
         int sizeofTuple1 = ((SortMerge) sm1).getFinalTupleSize();
          int sizeofTuple2 = ((SortMerge) sm2).getFinalTupleSize();
          
          System.out.println("Size of first iterator:"+sizeofTuple1);
          System.out.println("Size of second iterator:"+sizeofTuple2);
          
            
                    
                  String[] splited = conditions.get(0).split("\\s+");
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
        
                
                    String relationship = splited[2];
        
                    CondExpr[] outFilter = new CondExpr[3];
                    outFilter[0] = new CondExpr();
                    outFilter[1] = new CondExpr();
        
                    outFilter[0].next = null;
                    outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
                    outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                    outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
                    outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), index * 3 + 1);
                    outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
                    outFilter[0].flag = 1;
                    // outFilter[1] = null;
        
                  if(relationship.equals("PC")) {
                          
                          outFilter[1].next  = null;
                          outFilter[1].op    = new AttrOperator(AttrOperator.aopLT);
                          outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
                          outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
                          outFilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),index*3+2);
                
                          outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),5);
                          outFilter[1].flag=1;
                          outFilter[2] = null;
                  }
                  else if(relationship.equals("AD"))
                  {           
                      outFilter[1] = null;
                      outFilter[2] = null;                         
                  }
        
                  
                  AttrType [] ltypes = new AttrType[sizeofTuple1];
                  for(int j=0;j<sizeofTuple1;j=j+3) {
                        ltypes[j] = new AttrType(AttrType.attrInterval);
                        ltypes[j+1]=new AttrType(AttrType.attrInteger); 
                        ltypes[j+2]=new AttrType(AttrType.attrString);
                 }
                
                  short []  lsizes = new short[sizeofTuple1/3];
                  for(int j=0;j<lsizes.length;j++)
                      lsizes[j]=10;
                  
                  AttrType [] rtypes = new AttrType[sizeofTuple2];
                  for(int j=0;j<sizeofTuple2;j=j+3) {
                        rtypes[j] = new AttrType(AttrType.attrInterval);
                        rtypes[j+1]=new AttrType(AttrType.attrInteger); 
                        rtypes[j+2]=new AttrType(AttrType.attrString);
                 }
                
                  short []  rsizes = new short[sizeofTuple2/3];
                  for(int j=0;j<rsizes.length;j++)
                      rsizes[j]=10;
                
                  
                      int fieldCounts = sizeofTuple1+ 3;
                      FldSpec []  proj1 = new FldSpec[fieldCounts];
                    int n_out_fld=0;
                      //for outer relations
                      for(int i=0;i< sizeofTuple1;i=i+3) {
                          proj1[i]=new FldSpec(new RelSpec(RelSpec.outer), 1+i);
                          proj1[i+1]=new FldSpec(new RelSpec(RelSpec.outer), 2+i);
                          proj1[i+2]=new FldSpec(new RelSpec(RelSpec.outer), 3+i);
                     }
                      
                    //for inner relations
                      proj1[fieldCounts-3]=new FldSpec(new RelSpec(RelSpec.innerRel), 4);
                      proj1[fieldCounts-2]=new FldSpec(new RelSpec(RelSpec.innerRel), 5);
                      proj1[fieldCounts-1]=new FldSpec(new RelSpec(RelSpec.innerRel), 6);
                    
                    
                    try {
                              
                              n_out_fld = sizeofTuple1 + 3;
                              int joinColumn1= index * 3 + 1;
                              int joinColumn2= 4;
                                 
                              //System.out.println(joinColumn);
                              sm_final = new SortMerge(ltypes, ltypes.length, lsizes,
                                        rtypes, rtypes.length, rsizes,
                                        joinColumn1, 8, 
                                        joinColumn2, 8, 
                                        ((n_out_fld/3)-1)*10,
                                        sm1, sm2, 
                                        false, false, ascending,
                                        outFilter, proj1, n_out_fld);                     }
                          catch (Exception e) {
                                    System.err.println ("*** Error preparing for sm_join");
                                    System.err.println (""+e);
                                    e.printStackTrace();
                                    Runtime.getRuntime().exit(1);
                        }
                    
                       //   QueryPlanExecutor2(map, conditions, sm, conditionCount+1, heapFileName, dynamicCount, dynamic);    
                        
                          
          
          return (SortMerge)sm_final;
    }
    
    private SortMerge QueryPlanExecutor2_bushy_new(HashMap<Integer, String> map,List<String> conditions, HashMap<String, Iterator> listOfIterators) {
        
         int dynamicCount=-1;
        //add first condition to dynamic map
          HashMap<Integer,String> dynamic =new HashMap<Integer,String>();
          String[] splited = conditions.get(0).split("\\s+");
          dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[0])));
          dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[1])));

          TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
          SortMerge sm_final = null; 
          
              
      for(int conditionCount=1;conditionCount<=conditions.size()-1;conditionCount++){
              
              Iterator it_val1=null;
                if(conditionCount==1){
                    it_val1= listOfIterators.get(conditions.get(0));
                    
                }else{
                    it_val1 = sm_final;
                }
                    
                
                String condition_val2 = conditions.get(conditionCount);
                Iterator it_val2 = listOfIterators.get(conditions.get(conditionCount));
                
                int sizeofTuple1=-1;
                if(it_val1!=null){
                    sizeofTuple1  = ((SortMerge) it_val1).getFinalTupleSize();
                    
                }
                
                int sizeofTuple2 = ((SortMerge) it_val2).getFinalTupleSize();
                  
                
                //from 2nd condition adding it to dynamic hashmap
                
                String[] splited2 = condition_val2.split("\\s+");
                int index = 0;
                String notRepatedElement = null;
                int notRepatedElementIndex=-1;
                if (!dynamic.containsValue(map.get(Integer.valueOf(splited2[0])))|| map.get(Integer.valueOf(splited[0])).equals("*")) {
                    dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited2[0])));
                    notRepatedElement = map.get(Integer.valueOf(splited2[0]));
                    notRepatedElementIndex=0;
                } else {
                    for (Map.Entry<Integer, String> e : dynamic.entrySet()) {
                        if (e.getValue().equals(map.get(Integer.valueOf(splited2[0])))) {
                            index = e.getKey();
                            break;
                        }
                    }
                }
    
                if (!dynamic.containsValue(map.get(Integer.valueOf(splited2[1])))|| map.get(Integer.valueOf(splited[1])).equals("*")) {
                    dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited2[1])));
                    notRepatedElement = map.get(Integer.valueOf(splited2[1]));
                    notRepatedElementIndex=1;
                } else {
                    for (Map.Entry<Integer, String> e : dynamic.entrySet()) {
                        if (e.getValue().equals(map.get(Integer.valueOf(splited2[1])))) {
                            index = e.getKey();
                            break;
                        }
                    }
                }

                String relationship = splited2[2];
                
                CondExpr[] outFilter = new CondExpr[3];
                outFilter[0] = new CondExpr();
                outFilter[1] = new CondExpr();
    
                outFilter[0].next = null;
                outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
                outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
                outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
                outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), index * 3 + 1);
                outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), notRepatedElementIndex * 3 +1);
                outFilter[0].flag = 1;
                // outFilter[1] = null;
    
              if(relationship.equals("PC")) {
                      
                      outFilter[1].next  = null;
                      outFilter[1].op    = new AttrOperator(AttrOperator.aopLT);
                      outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
                      outFilter[1].type2 = new AttrType(AttrType.attrSymbol);
                      outFilter[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),index*3+2);
            
                      outFilter[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),notRepatedElementIndex*3+2);
                      outFilter[1].flag=1;
                      outFilter[2] = null;
              }
              else if(relationship.equals("AD"))
              {           
                  outFilter[1] = null;
                  outFilter[2] = null;                         
              }
    
       
              AttrType [] ltypes = new AttrType[sizeofTuple1];
              for(int j=0;j<sizeofTuple1;j=j+3) {
                    ltypes[j] = new AttrType(AttrType.attrInterval);
                    ltypes[j+1]=new AttrType(AttrType.attrInteger); 
                    ltypes[j+2]=new AttrType(AttrType.attrString);
             }
            
              short []  lsizes = new short[sizeofTuple1/3];
              for(int j=0;j<lsizes.length;j++)
                  lsizes[j]=10;
              
              AttrType [] rtypes = new AttrType[sizeofTuple2];
              for(int j=0;j<sizeofTuple2;j=j+3) {
                    rtypes[j] = new AttrType(AttrType.attrInterval);
                    rtypes[j+1]=new AttrType(AttrType.attrInteger); 
                    rtypes[j+2]=new AttrType(AttrType.attrString);
             }
            
              short []  rsizes = new short[sizeofTuple2/3];
              for(int j=0;j<rsizes.length;j++)
                  rsizes[j]=10;
            
              
                  int fieldCounts = sizeofTuple1+ 3;
                  FldSpec []  proj1 = new FldSpec[fieldCounts];
                int n_out_fld=0;
                  //for outer relations
                  for(int i=0;i< sizeofTuple1;i=i+3) {
                      proj1[i]=new FldSpec(new RelSpec(RelSpec.outer), 1+i);
                      proj1[i+1]=new FldSpec(new RelSpec(RelSpec.outer), 2+i);
                      proj1[i+2]=new FldSpec(new RelSpec(RelSpec.outer), 3+i);
                 }
                  
                //for inner relations
                  proj1[fieldCounts-3]=new FldSpec(new RelSpec(RelSpec.innerRel), notRepatedElementIndex*3+ 1);
                  proj1[fieldCounts-2]=new FldSpec(new RelSpec(RelSpec.innerRel), notRepatedElementIndex*3+ 2);
                  proj1[fieldCounts-1]=new FldSpec(new RelSpec(RelSpec.innerRel), notRepatedElementIndex*3+ 3);
                
                  try {
                      
                      n_out_fld = sizeofTuple1 + 3;
                      int joinColumn1= index * 3 + 1;
                      int joinColumn2= notRepatedElementIndex*3 +1;;
                         
                      //System.out.println(joinColumn);
                      sm_final = new SortMerge(ltypes, ltypes.length, lsizes,
                                rtypes, rtypes.length, rsizes,
                                joinColumn1, 8, 
                                joinColumn2, 8, 
//                                ((n_out_fld/3)-1)*40,
                                5000,
                                it_val1, it_val2, 
                                false, false, ascending,
                                outFilter, proj1, n_out_fld);                     }
                  catch (Exception e) {
                            System.err.println ("*** Error preparing for sm_join");
                            System.err.println (""+e);
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                }
                
      }
            
    return sm_final;
}
    
    
    
    public SortMerge  QueryPlanExecutor3(HashMap<Integer,String > map, List<String> conditions,Iterator it ,int conditionCount, String heapFileName,int dynamicCount,HashMap<Integer,String> dynamic_notneeded ) {
    
          HashMap<String,Iterator> listOfIterators= new HashMap<String,Iterator>();
          
          //nested join loop for each condition
          for(int conditionscount=0;conditionscount<conditions.size();conditionscount++){
                  NestedLoopsJoins inl = null;
                 HashMap<Integer,String> dynamic =new HashMap<Integer,String>();
                
                  List<String> condnew=new ArrayList<String>(); 
                  condnew.add(conditions.get(conditionscount));
                
                  SortMerge nlj=QueryPlanExecutor2_iterative(map, condnew, inl, 0, heapFileName, -1, dynamic);
                  listOfIterators.put(conditions.get(conditionscount), nlj);              
                
          }
          
          //sort merge for each iterator
          //set dynamic map and pass it as an argument
          
          HashMap<Integer,String> dynamic =new HashMap<Integer,String>();
          String[] splited = conditions.get(0).split("\\s+");
          dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[0])));
          dynamic.put(++dynamicCount, map.get(Integer.valueOf(splited[1])));
            
          
          List<String> condnew=new ArrayList<String>(); 
          condnew.add(conditions.get(1));
        //  condnew.add(  )    
          
         // SortMerge sm_final=QueryPlanExecutor2_bushy(map, condnew, listOfIterators.get(conditions.get(0)), listOfIterators.get(conditions.get(1)), 0, heapFileName, dynamicCount, dynamic);
         System.out.println("first part done.");
         SortMerge sm_final=QueryPlanExecutor2_bushy_new(map, conditions,listOfIterators);
            
          //printing iterator
        //  SortMerge nlj=(SortMerge) listOfIterators.get(conditions.get(1));
                  int sizeofTuple = sm_final.getFinalTupleSize();
                  
                  AttrType []  outputtype = new AttrType[sizeofTuple];
                    
                  for(int i=0;i< sizeofTuple;i=i+3) {
                      outputtype[i]= new AttrType(AttrType.attrInterval);
                      outputtype[i+1]=new AttrType(AttrType.attrInteger);
                      outputtype[i+2]=new AttrType(AttrType.attrString);
                        
                    }
                
                  Tuple t;
                    t = null;
                    
                    try {
                          
                    t = sm_final.get_next();
                    if(t!=null){
                        t.print(outputtype);
                        }
                    } catch (Exception e) {
                          System.err.println (""+e);
                          e.printStackTrace();
                          Runtime.getRuntime().exit(1);
                        }
            
                    
                    try {
                      while ((t = sm_final.get_next()) != null) {
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
                        sm_final.close();
                    }
                    catch (Exception e) {
                    
                      e.printStackTrace();
                    }
                    return sm_final;
          }

    public HashMap<Integer, String> getDynamic() {
        return dynamic;
    }

        }