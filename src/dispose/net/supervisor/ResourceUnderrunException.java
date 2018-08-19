package dispose.net.supervisor;

public class ResourceUnderrunException extends Exception
{
  private static final long serialVersionUID = 4722451453346775153L;

  
  public ResourceUnderrunException()
  {
    super();
  }


  public ResourceUnderrunException(String message)
  {
    super(message);
  }


  public ResourceUnderrunException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public ResourceUnderrunException(Throwable cause)
  {
    super(cause);
  }
}
