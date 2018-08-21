
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
import dispose.net.message.chkp.ChkpRequestMsg;
import dispose.net.message.chkp.ChkpResponseMsg;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.OperatorCheckpoint;
import dispose.net.node.operators.Operator;


/** Class representing a single operator thread. A thread is instantiated for
 * each operator on the node, and the threads are linked by using Links. */
public class OperatorThread extends ComputeThread
{
  private UUID jid;
  private Operator operator;

  private HashMap<Integer, MonitoredLink> inStreams = new HashMap<>();
  private OperatorBroadcast outLink = new OperatorBroadcast();

  HashMap<Integer, Integer> opIDtoLinkIdx = new HashMap<>();

  private int lastInputIdx = 0;

  private OperatorInputState barrier;

  private List<ConcurrentLinkedQueue<DataAtom>> inputQueues;

  private HashMap<UUID, OperatorCheckpoint> checkpoints = new HashMap<>();

  private AtomicBoolean running = new AtomicBoolean(false);

  private Thread processThread;
  
  
  public OperatorThread(Node owner, UUID jid, Operator operator)
  {
    super(owner);
    this.operator = operator;
    this.opID = operator.getID();
    this.jid = jid;
    
    int numInputs = operator.getNumInputs();

    inputQueues = new ArrayList<>(numInputs);

    for (int d = 0; d < numInputs; d++) {
      inputQueues.add(new ConcurrentLinkedQueue<>());
    }
    
    barrier = new OperatorInputState(inputQueues);
  }


  /** Set the input link for the given operator id
   * @param inputLink Input link to use
   * @param fromId ID of the upstream operator connected through this link
   * @throws IOException */
  public synchronized void setInputLink(Link inputLink, int fromId) throws ClosedEndException
  {
    boolean repair = inStreams.containsKey(fromId);

    Integer idx = lastInputIdx;

    if (repair) {
      inStreams.get(fromId).close();
      inStreams.remove(fromId);
    } else {
      opIDtoLinkIdx.put(fromId, lastInputIdx);
      lastInputIdx++;
    }

    idx = opIDtoLinkIdx.get(fromId);

    inStreams.put(fromId, MonitoredLink.asyncMonitorLink(inputLink, new OperatorInputDelegate(this, idx), 0));
  }

  
  /** Set the output link (there can be many) for the given downstream operator
   * @param outputLink The output link to use
   * @param toId The ID of the downstream operator connected through the link
   * @throws IOException */
  public synchronized void setOutputLink(Link outputLink, int toId) throws ClosedEndException
  {
    outLink.setOutputLink(outputLink, toId, new OperatorOutputDelegate(this));
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
    public void messageReceived(Message msg)
    {
      if (msg instanceof DataAtom) {
        this.op.notifyElement(StreamIndex, ((DataAtom) msg));
      } else if (msg instanceof ChkpRequestMsg) {
        this.op.notifyChkpMessage(StreamIndex, (ChkpRequestMsg) msg);
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


    public OperatorOutputDelegate(OperatorThread op)
    {
      this.op = op;
    }


    @Override
    public void messageReceived(Message msg)
    {
      // do nothing, we don't expect messages from downstream
    }


    @Override
    public void linkIsBroken(Exception e)
    {
      this.op.pause();
    }

  }

  public void reloadFromCheckPoint(OperatorCheckpoint checkpoint) {
    
    checkpoints.clear();
    
    inputQueues = checkpoint.getInFlight();
    operator = checkpoint.getOperator();
    barrier = checkpoint.getInputState();
    
  }

  /** Starts the operator, by starting all the links monitors and queues */
  @Override
  public void start()
  {
    this.running.set(true);
    
    this.processThread = new Thread(() -> process());
    processThread.start();
  }


  /** Pauses the execution, meaning that the operator will still read the data,
   * but won't process it (filling the input queue) */
  @Override
  public void pause()
  {
    DisposeLog.debug(this, "Operator ", opID, " paused");
    this.running.set(false);
  }


  /** Resumes the thread processing execution, allowing it to read from the
   * input queues. */
  @Override
  public void resume()
  {
    this.running.set(true);
    
    processThread = new Thread(() -> process());
    processThread.start();
  }


  @Override
  public void stop()
  {
    
    running.set(false);

    barrier.forceStop();
    
    for (MonitoredLink inLink : inStreams.values())
      inLink.close();
    
    outLink.close();
    
    try{
      processThread.join();
    } catch (InterruptedException e) { /* Who cares */ }
    
    DisposeLog.debug(this, "Operator ", opID, " stopped");
  }


  /** Performs the operations to deal with a new atom received from a upstream
   * operator
   * @param idx The index of the input link from which the atom was received
   * @param element The received atom */
  private void notifyElement(int idx, DataAtom element)
  {
    if (element != null) {
      this.inputQueues.get(idx).offer(element);
      synchronized (barrier) {
        barrier.notify();
      }

      // adds value to all current checkpoints
      if (!checkpoints.isEmpty()) {
        for (OperatorCheckpoint chkp : checkpoints.values()) {
          if (!chkp.isComplete())
            chkp.addInFlightAtom(idx, element);
        }
      }
    }
  }


  /** Perform the operations to deal with a checkpoint/snapshot message received
   * from upstream
   * @param idx The index of the input link from which the message was received
   * @param msg The received message */
  private synchronized void notifyChkpMessage(int idx, ChkpRequestMsg msg)
  {
    OperatorCheckpoint current;

    UUID id = msg.getCheckpointID();

    if (checkpoints.containsKey(id)) {
      current = checkpoints.get(id);
    } else {
      current = new OperatorCheckpoint(id, operator, barrier);
      checkpoints.put(id, current);

      // forwards checkpoint message to all downstream operators
      try {
          outLink.sendMsg(msg);
      } catch (Exception e) {
        DisposeLog.error(this, "Exception while processing checkpoint message ",
          e.getMessage());
        e.printStackTrace();
      }

    }

    current.notifyCheck(idx);

    if (current.isComplete()) {
      DisposeLog.debug(OperatorThread.class,
        "Checkpoint " + current.getID() + " completed on operator "
                                             + this.operator.getID() + ": "
                                             + current);

      owner.sendMsgToSupervisor(getID(), new ChkpResponseMsg(current));

      checkpoints.remove(id);
    }
  }

  
  /** Main loop of the thread. Gets the control commands and runs the operator
   * on each new data on the input link. */
  private void process()
  {
    while (this.running.get()) {
      
      // I/O processing
      try {
        DataAtom[] inputAtoms = barrier.recvAtomsBlocking();
        
        if (barrier.stopCondition()) {
          DisposeLog.debug(this, "Operator ", getID(), " received EndData");
          outLink.sendMsg(new EndData());
          pause();
          
        } else if (barrier.processCondition()) {
          // process the inputs
          List<DataAtom> result = this.operator.processAtom(inputAtoms);

          // sends non-null results to all the children streams
            for (DataAtom resAtom : result) {
              if (resAtom != null && !(resAtom instanceof NullData)) {
                outLink.sendMsg(resAtom);
              }
          }

          barrier.resetAfterProcessing();
        }

      } catch (Exception e) {
        DisposeLog.error(this, "Exception while processing ", e.getMessage());
        e.printStackTrace();
      }
    }
  }
}