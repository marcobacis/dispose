package dispose.net.message;

public class StartOperatorMsg extends CtrlMessage
{
  private static final long serialVersionUID = -2977267460391785421L;

  private int opID = 0;
  
  private boolean all = false;
  
  public StartOperatorMsg()
  {
    this.all = true;
  }
  
  public StartOperatorMsg(int id)
  {
    this.opID = id;
  }
  
  public boolean all()
  {
    return this.all;
  }
  
  public int id()
  {
    return this.opID;
  }
}
