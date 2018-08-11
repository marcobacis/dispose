package dispose.client;

import java.util.stream.Collectors;

public class FileProducerStream extends Stream
{
  private static final long serialVersionUID = 422454195691636339L;
  
  private String filepath;
  
  public FileProducerStream(String filepath) {
    super();
    this.filepath = filepath;
  }
  
  public String getFilePath() {
    return this.filepath;
  }
  
  @Override
  public String toString() {
    String id = Integer.toString(getID());
    String op = this.operation.getName();
    String win = Integer.toString(getWindowSize());
    String slide = Integer.toString(getWindowSlide());
    
    String childrensID = "(" + children.stream()
                               .map(child -> Integer.toString(child.getID()))
                               .collect(Collectors.joining(",")) + ")";
    
    String parentsID = "()"; // A producer has no parents
    
    return new String("(" + String.join(";", id, this.filepath, op, win, slide, parentsID, childrensID) + ")");
  }
  
}
