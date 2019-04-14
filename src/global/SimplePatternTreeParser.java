package global;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.NestedLoopsJoins;

public class SimplePatternTreeParser {
    private List<String> conditions;
    private HashMap<Integer,String > map;
    private HashMap<Integer,String> dynamic;
    NestedLoopsJoins inl = null;
    
    
	
    public HashMap<Integer, String> getMap() {
		return map;
	}

	public void setMap(HashMap<Integer, String> map) {
		this.map = map;
	}

	public HashMap<Integer, String> getDynamic() {
		return dynamic;
	}

	public void setDynamic(HashMap<Integer, String> dynamic) {
		this.dynamic = dynamic;
	}

	public NestedLoopsJoins getInl() {
		return inl;
	}

	public void setInl(NestedLoopsJoins inl) {
		this.inl = inl;
	}

	public List<String> getConditions() {
        return conditions;
    }

    public void setConditions(List<String> conditions) {
        this.conditions = conditions;
    }

    public SimplePatternTreeParser(String inputPath) {
        String st;
        BufferedReader br;
        List<String> conditions= new ArrayList<String>();
        HashMap<Integer,String> map= new HashMap<Integer,String>();
        HashMap<Integer,String> dynamic= new HashMap<Integer,String>();
		   
        
        try {
            br = new BufferedReader(new FileReader(inputPath));
            st = br.readLine();
            int nodesCount = Integer.valueOf(st);
            
            //NodeMap
            this.map= new HashMap<Integer,String>();
            
            for(int i=0;i<nodesCount;i++) {
                st = br.readLine();
                map.put(i+1, st);
            }
            this.map=map;
            
            //Conditions list
            while ((st = br.readLine()) != null) {
                conditions.add(st);
            }
            this.conditions=conditions;
            
            br.close();
            
            //HashMap to save the output tuple indexes
      	    this.dynamic=dynamic; 
		
            
        }catch (FileNotFoundException e) {
            System.out.println("Error: Invalid file path in pattern tree");
            return;
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
        this.conditions = conditions;
    }

}
