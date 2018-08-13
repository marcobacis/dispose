package dispose.log;

public enum LogPriority {
  FAILURE(1000),
  CRITICAL(900),
  ERROR(500),
  WARNING(300),
  INFO(200),
  DEBUG(0);
  
  private int orderNum;
  
  
  LogPriority(int orderNum)
  {
    this.orderNum = orderNum;
  }
  
  
  public int toInteger()
  {
    return orderNum;
  }
}
