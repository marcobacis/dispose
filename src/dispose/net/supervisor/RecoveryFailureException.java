package dispose.net.supervisor;

public class RecoveryFailureException extends Exception
{
  private static final long serialVersionUID = 4722451453346775153L;
  private boolean fatal = false;

  
  public RecoveryFailureException()
  {
    super();
  }


  public RecoveryFailureException(String message)
  {
    super(message);
  }


  public RecoveryFailureException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public RecoveryFailureException(Throwable cause)
  {
    super(cause);
  }
  
  
  public RecoveryFailureException(String message, Throwable cause, boolean fatal)
  {     
    super(message, cause);
    this.fatal = fatal;
  }
  
  
  public boolean isFatal()
  {
    return fatal;
  }
}
