package dispose.net.supervisor;

public class DeadJobException extends Exception
{
  private static final long serialVersionUID = 5332299428892402854L;


  public DeadJobException()
  {
    super();
  }


  public DeadJobException(String message)
  {
    super(message);
  }


  public DeadJobException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public DeadJobException(Throwable cause)
  {
    super(cause);
  }
}
