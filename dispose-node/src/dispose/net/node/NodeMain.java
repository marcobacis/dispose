
package dispose.net.node;

import java.io.*;

import dispose.net.common.Config;
import dispose.net.links.SocketLink;


public class NodeMain
{
  public static void main(String[] args) throws IOException, ClassNotFoundException
  {
    String host;
    if (args.length < 1) {
      System.out.println("usage: java dispose.net.node [supervisor-ip]");
      host = "127.0.0.1";
    } else {
      host = args[0];
    }
    SocketLink ctrl = SocketLink.connectTo(host, Config.nodeCtrlPort);

    Node node = new Node(ctrl);
    node.run();
  }
}
