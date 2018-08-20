package dispose.log;

import java.util.Date;

public class StdoutLogEmitter implements LogEmitter
{
  int highestPri = LogPriority.DEBUG.toInteger();
  
  
  public void setHighestPriority(LogPriority max)
  {
    highestPri = max.toInteger();
  }
  
  
  @Override
  public void emitMessage(LogPriority pri, Date date, String originator, String message)
  {
    if (pri.toInteger() < highestPri)
      return;
    
    String line = date.toString() + ": [" + originator + "] " + message;
    if (pri.toInteger() >= LogPriority.ERROR.toInteger())
      System.err.println(line);
    else
      System.out.println(line);
  }

}
