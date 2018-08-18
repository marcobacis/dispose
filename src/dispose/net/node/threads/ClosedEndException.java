package dispose.net.node.threads;

public class ClosedEndException extends Exception
{
  private static final long serialVersionUID = 4214904305540577366L;


  public ClosedEndException()
  {
    super();
  }


  public ClosedEndException(String message)
  {
    super(message);
  }


  public ClosedEndException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public ClosedEndException(Throwable cause)
  {
    super(cause);
  }
}
