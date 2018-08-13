package dispose.log;

import java.util.Date;

public interface LogEmitter
{
  public void emitMessage(LogPriority pri, Date date, String originator, String message);
}
