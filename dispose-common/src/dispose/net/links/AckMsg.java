package dispose.net.links;

import java.util.UUID;

import dispose.net.message.Message;

class AckMsg extends Message
{
  private static final long serialVersionUID = 7115604268703017918L;
  UUID acknowledgedMsgUUID;
  
  
  AckMsg(UUID ackUUID)
  {
    acknowledgedMsgUUID = ackUUID;
  }

  
  UUID getAcknowledgedUUID()
  {
    return acknowledgedMsgUUID;
  }
  
  
  @Override
  public UUID getUUID()
  {
    return acknowledgedMsgUUID;
  }
}
