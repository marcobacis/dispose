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
    System.out.println(date.toString() + ": [" + originator + "] " + message);
  }

}
