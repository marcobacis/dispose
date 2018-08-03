package dispose.net.message;

public class ConnectOperatorMsg extends CtrlMessage
{

  private static final long serialVersionUID = 2787870806083718206L;

  private int fromID;
  private int toID;
  
  public ConnectOperatorMsg(int from, int to)
  {
    this.fromID = from;
    this.toID = to;
  }
  
  public int getFrom()
  {
    return this.fromID;
  }
  
  public int getTo()
  {
    return this.toID;
  }

}
