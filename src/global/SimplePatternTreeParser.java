package global;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SimplePatternTreeParser {
    private List<String> conditions;
    
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
        
        try {
            br = new BufferedReader(new FileReader(inputPath));
            st = br.readLine();
            int nodesCount = Integer.valueOf(st);
            
            HashMap<Integer,String > map= new HashMap<Integer,String>();
            
            for(int i=0;i<nodesCount;i++) {
                st = br.readLine();
                map.put(i+1, st);
            }
            while ((st = br.readLine()) != null) {
                conditions.add(st);
            }
            br.close();
            
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
