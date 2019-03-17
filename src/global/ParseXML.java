package global;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import heap.Heapfile;
import heap.NodeTuple;
import heap.Scan;
import heap.Tuple;


public class ParseXML {
	public static String path = "/Users/sidmadan/Documents/cse510/xml_sample_data.xml";	
	public static final int min = Integer.MIN_VALUE;
	
	public static NodeTuple convertElementToNode(Element element, int level, String name) {
		NodeTuple n = new NodeTuple();
		Intervaltype interval = new Intervaltype();
		interval.setStart(min);
		interval.setEnd(min);
		n.setNodeTag(element);
		n.setNodeIntLabel(interval);
		n.setLevel(level);
		n.setName(name);
		n.setNodeType(0);
		return n;
	}
    public static NodeTuple convertElementToNode(Element element, int level, String name, int nodeT) {
        NodeTuple n = new NodeTuple();
        Intervaltype interval = new Intervaltype();
        interval.setStart(min);
        interval.setEnd(min);
        n.setNodeTag(element);
        n.setNodeIntLabel(interval);
        n.setLevel(level);
        n.setName(name);
        n.setNodeType(nodeT);
        return n;
    }
	
	
    public static List<NodeTuple> parse(String path) throws Exception {
        int counter = -100000;
        int level;
        Stack<NodeTuple> stack = new Stack<>();
        List<NodeTuple> nodes = new ArrayList<NodeTuple>();
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();

        Document doc = db.parse(new FileInputStream(new File(path)));
        
        Element root = doc.getDocumentElement();
        
        stack.add(convertElementToNode(root, 0 , root.getNodeName()));
        NodeTuple node;
        Intervaltype interval;
        while(!stack.isEmpty()) {
    	  node = stack.pop();
    	  
    	  interval = node.getNodeIntLabel();
    	  level = node.getLevel();
    	  
    	  if(interval.getStart() == min) {
	    	  interval.setStart(counter++);
	    	  
	    	  node.setNodeIntLabel(interval);
	    	  if (node.getNodeType() == 1) {
	    	      Intervaltype interval2;
	    	      NodeTuple node2;
	    	      node2 = stack.pop();
	    	      if (node2.getNodeType() == 2) {
	    	          interval2 = node2.getNodeIntLabel();
	    	          interval2.setStart(counter++);
	    	          interval2.setEnd(counter++);
	    	          node2.setNodeIntLabel(interval2);
	    	          System.out.println("Found element " + node2.getName() + " "
	                          +  node2.getNodeIntLabel().getStart() + " " + node2.getLevel() + " " + node2.getNodeIntLabel().getEnd() + " " +  node2.getName());

	    	          nodes.add(node2);
	    	      } else {
	    	          stack.push(node2);
	    	      }
	    	      interval.setEnd(counter++);
	    	      node.setNodeIntLabel(interval);
	    	      nodes.add(node);
	    	      System.out.println("Found element " + node.getName() + " "
	                      +  node.getNodeIntLabel().getStart() + " " + node.getLevel() + " " + node.getNodeIntLabel().getEnd() + " " +  node.getName());

	    	      continue;
	    	      
	    	  }

	    	  stack.push(node);
	    	  if( node.getNodeTag() != null) {
	    		  NodeList entries = node.getNodeTag().getChildNodes();
	    		  
    	    		  if (node.getNodeTag().getChildNodes().item(0).getNodeType() == Node.TEXT_NODE
    	    				  && node.getNodeTag().getChildNodes().getLength() == 1 ) {
    	    		      stack.push(convertElementToNode(null, level + 1, node.getNodeTag().getTextContent()));  
    	    		  }
    	    		  
    	    		  if( node.getNodeTag().hasAttributes() ) {
                          NamedNodeMap namedNodeMap   = node.getNodeTag().getAttributes();
                        System.out.println("Found element " + namedNodeMap.getLength());
                            for(int j = 0 ; j < namedNodeMap.getLength() ; j++) {
    
                                     Attr attrname = (Attr) namedNodeMap.item(j);
                                     
    
                                    stack.push(convertElementToNode( null, level + 2, attrname.getValue(), 2 ));                   
                                    stack.push(convertElementToNode(null, level + 1, attrname.getName(), 1));

                            }       
                        } 
    		          for (int i = entries.getLength() -1; i >= 0; i--) {
    		        	  if(entries.item(i).getNodeType() == Node.ELEMENT_NODE) {
    		                    Element element = (Element) entries.item(i);
   		    	            
    		    	            
                                stack.push(convertElementToNode(element, level + 1, element.getNodeName()));


    		    	            
    		        	  } 
    		          }
	    		  
	         }
	    		  
	          
    	  } else {
    		  
    		  interval = node.getNodeIntLabel();
    		  interval.setEnd(counter++);
    		  node.setNodeIntLabel(interval);
   			  System.out.println("Found element " + node.getName() + " "
   					  +  node.getNodeIntLabel().getStart() + " " + node.getLevel() + " " + node.getNodeIntLabel().getEnd() + " " +  node.getName());
   			  nodes.add(node);

    			  
    		  
    		  // todo: for saving tuple 
    		  
    		  
    	  }
        }
        return nodes;
//            NodeList children = element.getChildNodes();
            
//            for (int childNo = 0; childNo < (children.getLength()); childNo++) {
//            	//Element child = (Element) children.item(childNo);
//            	System.out.println(children.item(childNo).getNodeName());
//            }

            /*
             Node childNode = childNodes.item(childNo);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                Element elem = (Element) childNode;

                Log.d(elem.getNodeValue(), "Zed");
            }
            */

    }
    
    
    
    public static void main(String[] args) {
    	System.out.println("runignsgisibgissnigs");
    	try {
    		parse(path);
    	}  catch(Exception e) {
	    	  e.printStackTrace();
	      }
    }

}
