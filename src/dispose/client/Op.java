package dispose.client;

public enum Op
{
  NONE("NONE"),
  SUM("SUM"),
  AVG("AVG"),
  MAX("MAX"),
  MIN("MIN");
  
  private String name;
  
  private Op(String name) {
    this.name = name;
  }
  
  public String getName() {
    return this.name;
  }
}
