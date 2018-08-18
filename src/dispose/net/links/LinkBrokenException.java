
package dispose.net.links;

public class LinkBrokenException extends Exception
{
  private static final long serialVersionUID = 6390625057043022915L;


  public LinkBrokenException()
  {
    super();
  }


  public LinkBrokenException(String message)
  {
    super(message);
  }


  public LinkBrokenException(String message, Throwable cause)
  {     
    super(message, cause);
  }


  public LinkBrokenException(Throwable cause)
  {
    super(cause);
  }
}
