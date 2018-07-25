package dispose.client;

import java.util.stream.Collectors;

public class FileConsumerStream extends Stream
{
  private static final long serialVersionUID = 1389004132285518627L;
  private String filepath;
  
  public FileConsumerStream(String filepath, Stream inStream) {
    super(Op.NONE, 1, inStream);
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
    
    String childrensID = "()";
    
    String parentsID = "(" + parents.stream()
                               .map(parent -> Integer.toString(parent.getID()))
                               .collect(Collectors.joining(",")) + ")";
    
    return new String("(" + String.join(";", id, this.filepath, op, win, slide, parentsID, childrensID) + ")");
  }
}
