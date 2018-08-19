package dispose.net.supervisor;

public class InvalidDagException extends Exception
{
  private static final long serialVersionUID = -1708842341388447711L;
  
  
  public InvalidDagException()
  {
    super();
  }


  public InvalidDagException(String message)
  {
    super(message);
  }


  public InvalidDagException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public InvalidDagException(Throwable cause)
  {
    super(cause);
  }
}
