package dispose.net.message;

import java.util.UUID;

import dispose.net.node.Node;

public class CompletedJobMsg extends CtrlMessage
{

  private static final long serialVersionUID = -6625358498573472108L;
  private UUID jid;
  
  public CompletedJobMsg(UUID jid)
  {
    this.jid = jid;
  }
  
  @Override
  public void executeOnNode(Node node) throws MessageFailureException
  {
    node.notifyCompletedJob(jid);
  }
}
