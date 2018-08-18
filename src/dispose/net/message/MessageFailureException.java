package dispose.net.message;

public class MessageFailureException extends Exception
{
  private static final long serialVersionUID = 6390625057043022915L;


  public MessageFailureException()
  {
    super();
  }


  public MessageFailureException(String message)
  {
    super(message);
  }


  public MessageFailureException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public MessageFailureException(Throwable cause)
  {
    super(cause);
  }
}
