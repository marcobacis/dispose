
package dispose.net.node.threads;

import java.io.IOException;
import java.util.HashMap;

import dispose.net.links.Link;
import dispose.net.links.LinkBrokenException;
import dispose.net.links.MonitoredLink;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;


public class OperatorBroadcast
{

  private HashMap<Integer, MonitoredLink> outStreams = new HashMap<>();


  /** Set the output link (there can be many) for the given downstream operator
   * @param outputLink The output link to use
   * @param toId The ID of the downstream operator connected through the link
   * @throws IOException */
  public synchronized void setOutputLink(Link outputLink, int toId,
    Delegate delegate) throws ClosedEndException
  {
    if (outStreams.containsKey(toId)) {
      outStreams.get(toId).close();
      outStreams.remove(toId);
    }

    outStreams.put(toId, MonitoredLink.asyncMonitorLink(outputLink, delegate));
  }


  /** Sends the message to all the downstream operators
   * @param message The message to send
   * @throws LinkBrokenException */
  public void sendMsg(Message message) throws LinkBrokenException
  {
    for (MonitoredLink out : outStreams.values()) {
      out.sendMsg(message);
    }
  }


  /** Closes the link to all downstream operators */
  public void close()
  {
    for (MonitoredLink outLink : outStreams.values())
      outLink.close();
  }

}
