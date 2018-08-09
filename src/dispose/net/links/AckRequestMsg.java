package dispose.net.links;

import dispose.net.message.CtrlMessage;

class AckRequestMsg extends CtrlMessage
{
  private static final long serialVersionUID = -7459338247048033324L;
  CtrlMessage wrappedMsg;
  
  
  AckRequestMsg(CtrlMessage wrappedMsg)
  {
    this.wrappedMsg = wrappedMsg;
  }
  
  
  CtrlMessage getMessage()
  {
    return wrappedMsg;
  }
}
