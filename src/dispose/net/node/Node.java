package dispose.net.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import dispose.net.links.Link;
import dispose.net.links.PipeLink;
import dispose.net.links.SocketLink;
import dispose.net.message.ConnectOperatorMsg;
import dispose.net.message.ConnectRemoteOperatorMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.DeployOperatorMsg;
import dispose.net.message.StartOperatorMsg;


public class Node implements Runnable
{
  private boolean running = true;
  private Map<Integer, OperatorThread> operators;
  private Link ctrlLink;
  
  
  public Node(Link ctrlLink)
  {
    operators = new HashMap<>();
    this.ctrlLink = ctrlLink;
  }

  
  @Override
  public void run()
  {
    try {
      eventLoop();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  //TODO handle errors on the supervisor link (and on the creation of operator links)
  private void eventLoop() throws IOException
  {
    do {
      CtrlMessage msg;
      try {
        msg = (CtrlMessage) ctrlLink.recvMsg();
      } catch (ClassNotFoundException | IOException e1) {
        e1.printStackTrace();
        return;
      }

      //control messages parser
      //TODO move this into the messages classes?
      
      //Create an operator + thread
      if (msg instanceof DeployOperatorMsg) {
        DeployOperatorMsg castmsg = (DeployOperatorMsg) msg;
        operators.put(castmsg.getOperator().getID(), new OperatorThread(castmsg.getOperator()));
        
      }
      
      //Connect two local operators
      else if (msg instanceof ConnectOperatorMsg) {
        ConnectOperatorMsg castmsg = (ConnectOperatorMsg) msg;
        Link pipeLink = new PipeLink();
        
        operators.get(castmsg.getFrom()).addOutput(pipeLink);
        operators.get(castmsg.getTo()).addInput(pipeLink);
        
      }
      
      //Connect to/from remote operator
      else if (msg instanceof ConnectRemoteOperatorMsg) {
        ConnectRemoteOperatorMsg castmsg = (ConnectRemoteOperatorMsg) msg;
                
        if(operators.containsKey(castmsg.getFrom())) {
          SocketLink link = SocketLink.connectTo(castmsg.getRemoteHost(), castmsg.port());
          operators.get(castmsg.getFrom()).addOutput(link);
        } else {
          SocketLink link = SocketLink.connectFrom(castmsg.port());
          operators.get(castmsg.getTo()).addInput(link);
        }
        
      }
      
      //Start operator thread
      else if (msg instanceof StartOperatorMsg) {
        StartOperatorMsg castmsg = (StartOperatorMsg) msg;
        
        if(castmsg.all()) {
          for(Map.Entry<Integer, OperatorThread> e : operators.entrySet()) {
            e.getValue().start();
          }
        } else {
          operators.get(castmsg.id()).start();
        }
      }
      

    } while (running);
  }

}
