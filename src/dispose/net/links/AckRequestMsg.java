package dispose.net.links;

import java.util.UUID;

import dispose.net.message.Message;

class AckRequestMsg extends Message
{
  private static final long serialVersionUID = -7459338247048033324L;
  Message wrappedMsg;
  
  
  AckRequestMsg(Message wrappedMsg)
  {
    this.wrappedMsg = wrappedMsg;
  }
  
  
  Message getMessage()
  {
    return wrappedMsg;
  }
  
  
  @Override
  public UUID getUUID()
  {
    return wrappedMsg.getUUID();
  }
}
