package global;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class ComplexPatternTreeParser {
    private List<String> conditions1;
    public List<String> getConditions1() {
        return conditions1;
    }

    public void setConditions1(List<String> conditions1) {
        this.conditions1 = conditions1;
    }

    public List<String> getConditions2() {
        return conditions2;
    }

    public void setConditions2(List<String> conditions2) {
        this.conditions2 = conditions2;
    }

    public int getOperation() {
        return operation;
    }

    public void setOperation(int operation) {
        this.operation = operation;
    }

    public int getBuf_size() {
        return buf_size;
    }

    public void setBuf_size(int buf_size) {
        this.buf_size = buf_size;
    }

    private List<String> conditions2;
    private int operation;
    private int buf_size;
    
    public ComplexPatternTreeParser(String inputPath) {
        BufferedReader br;
        
        try {
            Path path = Paths.get(inputPath);
            long lineCount = Files.lines(path).count();
            String ptPath1;
            br = new BufferedReader(new FileReader(inputPath));
            
            ptPath1 = br.readLine();
            SimplePatternTreeParser spt = new SimplePatternTreeParser(ptPath1.trim());
            conditions1 = spt.getConditions();
            
            if(lineCount != 4 && lineCount != 3) {
                System.out.println("Error: Invalid Complex pattern tree format");
                br.close();
                return;
            }
            
            if (lineCount == 4) {
                String ptPath2;              
                ptPath2 = br.readLine();
                SimplePatternTreeParser spt2 = new SimplePatternTreeParser(ptPath2.trim());
                conditions2 = spt2.getConditions();
                
            } 
            operation = Integer.parseInt(br.readLine().trim());
            
            buf_size = Integer.parseInt(br.readLine().trim());
            br.close();
            
        }catch (FileNotFoundException | NoSuchFileException e ) {
            System.out.println("Error: Invalid file path in pattern tree");
            return;
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return;
        }
    }
    

}
