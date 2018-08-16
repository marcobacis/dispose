
package dispose.net.node.threads;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import dispose.log.DisposeLog;
import dispose.net.common.*;
import dispose.net.common.types.*;
import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.links.MonitoredLink.Delegate;
import dispose.net.message.Message;
import dispose.net.node.AtomsCache;
import dispose.net.node.ComputeThread;
import dispose.net.node.operators.Operator;


/** Class representing a single operator thread. A thread is instantiated for
 * each operator on the node, and the threads are linked by using Links. */
public class OperatorThread extends ComputeThread
{
  private Operator operator;

  private List<Link> inLinks = new ArrayList<>();
  private List<Link> outLinks = new ArrayList<>();

  private List<MonitoredLink> inStreams = new ArrayList<>();
  private List<MonitoredLink> outStreams = new ArrayList<>();

  private List<AtomsCache> outCaches = new ArrayList<>();

  private DataAtom[] inputAtoms;

  private List<ConcurrentLinkedQueue<DataAtom>> inputQueues;

  private AtomicBoolean running = new AtomicBoolean(false);


  public OperatorThread(Operator operator)
  {
    this.operator = operator;
    this.opID = operator.getID();
  }


  /** Adds an input link to get the atoms from
   * @param inputLink Input link to use
   * @throws IOException */
  public void addInput(Link inputLink) throws IOException
  {
    this.inLinks.add(inputLink);
  }


  /** Adds an output link (there can be many) to write the results to
   * @param outputLink The output link to use
   * @throws IOException */
  public void addOutput(Link outputLink) throws IOException
  {
    this.outLinks.add(outputLink);
    this.outCaches.add(new AtomsCache());
  }


  /** Pauses the execution, meaning that the operator will still read the data,
   * but won't process it (filling the input queue) */
  public void pause()
  {
    this.running.set(false);
  }


  /** Resumes the thread processing execution, allowing it to read from the
   * input queues. */
  public void resume()
  {
    this.running.set(true);
  }


  /** Input links delegate, responsible for asynchronously receiving DataAtoms
   * from upstream operators and filling the input queues. */
  private class OperatorInputDelegate implements Delegate
  {
    OperatorThread op;
    int StreamIndex;


    OperatorInputDelegate(OperatorThread op, int idx)
    {
      this.op = op;
      this.StreamIndex = idx;
    }


    @Override
    public void messageReceived(Message msg) throws Exception
    {

      if (msg instanceof DataAtom) {
        this.op.notifyElement(StreamIndex, ((DataAtom) msg));
        this.op.process();
      }
    }


    @Override
    public void linkIsBroken(Exception e)
    {
      this.op.pause();
      DisposeLog.error(this, "The ", this.StreamIndex, "th link on operator ",
        op.getID(), " is broken");
    }
  }


  /** Output links delegate, responsible for receiving data acks from downstream
   * operators and noticing links down. */
  private class OperatorOutputDelegate implements Delegate
  {

    private OperatorThread op;
    private int idx;


    public OperatorOutputDelegate(OperatorThread op, int idx)
    {
      this.op = op;
      this.idx = idx;
    }


    @Override
    public void messageReceived(Message msg) throws Exception
    {
      if (msg instanceof DataAtom) {
        DataAtom atomAck = (DataAtom) msg;
        this.op.notifyAck(this.idx, atomAck);
      }
    }


    @Override
    public void linkIsBroken(Exception e)
    {
      this.op.pause();
    }

  }


  /** Starts the operator, by starting all the links monitors and queues */
  public void start()
  {
    this.inputAtoms = new DataAtom[this.inLinks.size()];
    this.inputQueues = new ArrayList<>(this.inLinks.size());

    for (int d = 0; d < this.inLinks.size(); d++) {
      inputAtoms[d] = new NullData();
      this.inputQueues.add(new ConcurrentLinkedQueue<>());
    }

    for (int i = 0; i < this.inLinks.size(); i++) {
      this.inStreams.add(MonitoredLink.asyncMonitorLink(this.inLinks.get(i),
        new OperatorInputDelegate(this, i)));
    }

    for (int i = 0; i < this.outLinks.size(); i++) {
      this.outStreams.add(MonitoredLink.asyncMonitorLink(this.outLinks.get(i),
        new OperatorOutputDelegate(this, i)));
    }

    assert (this.outCaches.size() == this.outStreams.size());
    assert (this.inStreams.size() == this.operator.getNumInputs());

    this.running.set(true);
  }


  /** Performs the operations to deal with a new atom received from a upstream
   * operator
   * @param idx The index of the input link from which the atom was received
   * @param element The received atom */
  private void notifyElement(int idx, DataAtom element)
  {
    if (element != null)
      this.inputQueues.get(idx).offer(element);
  }


  /** Performs the operations to deal with an ack received from downstreams
   * operators
   * @param idx The index of the output link from which the ack was received
   * @param ack The ACKed atom */
  private synchronized void notifyAck(int idx, DataAtom ack)
  {
    this.outCaches.get(idx).acked(ack);
  }

  /** Main loop of the thread. Gets the control commands and runs the operator
   * on each new data on the input link. */
  private void process()
  {
    if (this.running.get()) {
      // I/O processing
      try {

        // grabs inputs from the queues
        for (int i = 0; i < this.inputAtoms.length; i++) {
          DataAtom inAtom = this.inputQueues.get(i).poll();
          if (inAtom != null)
            inputAtoms[i] = inAtom;
          else
            inputAtoms[i] = new NullData();
        }

        // process the inputs
        List<DataAtom> result = this.operator.processAtom(inputAtoms);

        // sends non-null results to all the children streams
        if (result.size() > 0) {
          for (DataAtom resAtom : result) {
            if (resAtom != null && !(resAtom instanceof NullData)) {
              for (int idx = 0; idx < this.outStreams.size(); idx++) {
                MonitoredLink out = this.outStreams.get(idx);
                out.sendMsg(resAtom);
                this.outCaches.get(idx).push(resAtom);
              }
            }
          }
        }

      } catch (Exception e) {
        DisposeLog.error(this, "Exception while processing ", e.getMessage());
        e.printStackTrace();
      }
    }
  }

}
