
package dispose.net.node;

import java.io.*;
import java.util.*;

import dispose.net.common.Config;
import dispose.net.links.Link;
import dispose.net.links.PipeLink;
import dispose.net.links.SocketLink;
import dispose.net.message.ConnectOperatorMsg;
import dispose.net.message.ConnectRemoteOperatorMsg;
import dispose.net.message.CtrlMessage;
import dispose.net.message.DeployOperatorMsg;
import dispose.net.message.StartOperatorMsg;


public class NodeMain
{

  private static boolean running = true;


  //TODO handle errors on the supervisor link (and on the creation of operator links)
  public static void main(String[] args) throws IOException, ClassNotFoundException
  {

    Map<Integer, OperatorThread> operators = new HashMap<>();
    
    SocketLink ctrl = SocketLink.connectFrom(Config.nodeCtrlPort);

    do {
      CtrlMessage msg = (CtrlMessage) ctrl.recvMsg();

      //ctrl messages parser
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
