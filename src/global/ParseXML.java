package global;

import java.io.File;
import java.io.FileInputStream;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import heap.NodeTuple;


public class ParseXML {
	static String path = "/Users/sidmadan/Documents/cse510/xml_sample_data.xml";	
	
	public static final int min = Integer.MIN_VALUE;
	
	public static NodeTuple convertElementToNode(Element element) {
		NodeTuple n = new NodeTuple();
		Intervaltype interval = new Intervaltype();
		interval.setStart(min);
		interval.setEnd(min);
		n.setNodeTag(element);
		n.setNodeIntLabel(interval);
		return n;
	}
	
    public static void parse(String path) throws Exception {
        int counter = -100000;
        Stack<NodeTuple> stack = new Stack<>();
        
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        DocumentBuilder db = dbf.newDocumentBuilder();

        Document doc = db.parse(new FileInputStream(new File(path)));
        
        Element root = doc.getDocumentElement();
        
        stack.add(convertElementToNode(root));
        NodeTuple node;
        Intervaltype interval;
        while(!stack.isEmpty()) {
    	   node = stack.pop();
    	  
    	  interval = node.getNodeIntLabel();
    	  if(interval.getStart() == min) {
	    	  interval.setStart(counter++);
	    	  
	    	  node.setNodeIntLabel(interval);
	    	  stack.push(node);
	    	  NodeList entries = node.getNodeTag().getChildNodes();
    	    
	          for (int i=entries.getLength() -1; i >= 0; i--) {
	        	  if(entries.item(i).getNodeType() == Node.ELEMENT_NODE){
	                    Element element = (Element) entries.item(i);
	    	            stack.push(convertElementToNode(element));
	        	  }     
	          }
	          
    	  } else {
    		  
    		  interval = node.getNodeIntLabel();
    		  interval.setEnd(counter++);
    		  node.setNodeIntLabel(interval);
    		  if(node.getNodeTag().getNodeType() == Node.ELEMENT_NODE){
    			  System.out.println("Found element " + node.getNodeTag().getTextContent() + " "
    					  +  node.getNodeIntLabel().getStart() + " " + node.getNodeIntLabel().getEnd());
    		  }
    		  // todo: for saving tuple 
    		  
    	  }
        }
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
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }

}
