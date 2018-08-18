package dispose.net.links;

public class NotAcknowledgeableException extends Exception
{
  private static final long serialVersionUID = 6390625057043022915L;


  public NotAcknowledgeableException()
  {
    super();
  }


  public NotAcknowledgeableException(String message)
  {
    super(message);
  }


  public NotAcknowledgeableException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public NotAcknowledgeableException(Throwable cause)
  {
    super(cause);
  }
}
