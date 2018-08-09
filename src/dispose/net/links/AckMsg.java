package dispose.net.links;

import java.util.UUID;

import dispose.net.message.CtrlMessage;

class AckMsg extends CtrlMessage
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
}
