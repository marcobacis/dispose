
package dispose.net.node;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.net.common.*;
import dispose.net.common.types.*;
import dispose.net.links.Link;

/**
 * Class representing a single operator thread.
 * A thread is instantiated for each operator on the node,
 * and the threads are linked by using Links.
 *
 */
public class OperatorThread extends Thread
{

  private Operator operator;

  private ObjectInputStream inStream;
  private List<ObjectOutputStream> outStreams;

  private ObjectInputStream ctrlIn;
  private ObjectOutputStream ctrlOut;

  private AtomicBoolean running = new AtomicBoolean(true);


  public OperatorThread(Operator operator)
  {
    this.operator = operator;
    this.outStreams = new ArrayList<>();
  }


  /**
   * Sets the control link from the node manager / supervisor
   * @param ctrlLink    Control link to use
   * @throws IOException
   */
  public void setCtrlLink(Link ctrlLink) throws IOException
  {
    this.ctrlIn = (ObjectInputStream) ctrlLink.getInputStream();
    this.ctrlOut = (ObjectOutputStream) ctrlLink.getOutputStream();
  }


  /**
   * Adds an input link to get the atoms from
   * @param inputLink   Input link to use
   * @throws IOException
   */
  public void addInput(Link inputLink) throws IOException
  {
    this.inStream = (ObjectInputStream) inputLink.getInputStream();
  }


  /**
   * Adds an output link (there can be many) to write the results to
   * @param outputLink  The output link to use
   * @throws IOException
   */
  public void addOutput(Link outputLink) throws IOException
  {
    this.outStreams.add((ObjectOutputStream) outputLink.getOutputStream());
  }

  /**
   * Sets the current cycle of the thread as the last one
   */
  public void kill()
  {
    this.running.set(false);
  }


  /**
   * Main loop of the thread. Gets the control commands
   * and runs the operator on each new data on the input link.
   */
  public void run() {


    while(running.get()) {

      //TODO handle ctrl link usage

      // I/O processing
      try {
        DataAtom inAtom = (DataAtom) this.inStream.readObject();

        DataAtom result = this.operator.processAtom(inAtom);

        if(!(result instanceof NullData)) {
          for(ObjectOutputStream out :  this.outStreams) {
            out.writeObject(result);
            out.flush();
          }
        }
      } catch(ClassNotFoundException e){
        System.out.println("Received object class not found! " + e.getMessage());
        e.printStackTrace();
      } catch (IOException e) {
        System.out.println("IOException: " +e.getMessage());
        e.printStackTrace();
      }

    }

  }

}
