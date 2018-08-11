package dispose.net.links;

import java.util.UUID;

import dispose.net.links.MonitoredLink.AckType;
import dispose.net.message.Message;

class AckRequestMsg extends Message
{
  private static final long serialVersionUID = -7459338247048033324L;
  private Message wrappedMsg;
  private AckType type;
  
  
  AckRequestMsg(Message wrappedMsg, AckType type)
  {
    this.wrappedMsg = wrappedMsg;
    this.type = type;
  }
  
  
  AckType getType()
  {
    return type;
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
