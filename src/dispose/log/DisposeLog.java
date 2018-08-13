package dispose.log;

import java.util.Date;

public class DisposeLog
{
  private static LogEmitter standardEmitter = new StdoutLogEmitter();
  
  
  public static void setLogEmitter(LogEmitter newEmitter)
  {
    if (newEmitter == null)
      throw new NullPointerException();
    DisposeLog.standardEmitter = newEmitter;
  }
  
  
  public static void log(LogPriority pri, Object origin, Object[] msg)
  {
    Date date = new Date();
    
    if (pri == null)
      pri = LogPriority.CRITICAL;
    
    String sorigin;
    if (origin == null) {
      sorigin = "<null>";
    } if (origin instanceof String) {
      sorigin = (String)origin;
    } else if (origin instanceof Class) {
      sorigin = ((Class<?>)origin).getSimpleName();
    } else {
      sorigin = origin.getClass().getSimpleName();
    }
    
    StringBuilder smsg = new StringBuilder();
    for (Object mobj: msg) {
      if (mobj == null)
        smsg.append("<null>");
      else
        smsg.append(mobj.toString());
    }
    
    DisposeLog.standardEmitter.emitMessage(pri, date, sorigin, smsg.toString());
  }
  
  
  public static void debug(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.DEBUG, origin, msg);
  }
  
  
  public static void info(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.INFO, origin, msg);
  }
  
  
  public static void warn(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.WARNING, origin, msg);
  }
  
  
  public static void error(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.ERROR, origin, msg);
  }
  
  
  public static void critical(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.CRITICAL, origin, msg);
  }
  
  
  public static void fail(Object origin, Object... msg)
  {
    DisposeLog.log(LogPriority.FAILURE, origin, msg);
  }
}
