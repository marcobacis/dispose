package dispose.net.supervisor;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import dispose.net.common.Config;
import dispose.net.node.checkpoint.Checkpoint;

public class CheckpointArchive
{
  private List<UUID> checkpointList = new ArrayList<>();
  private Map<UUID, Map<Integer, CheckpointPartProxy>> checkpoints = new HashMap<>();
  
  
  private class CheckpointPartProxy
  {
    private Path path;
    
    
    private CheckpointPartProxy(Checkpoint ckp) throws IOException
    {
      String uuid = ckp.getID().toString();
      String part = Integer.toHexString(ckp.getComputeNode().getID());
      Path ckpdir = Config.checkpointDataRoot.resolve(uuid);
      
      Files.createDirectories(ckpdir);
      this.path = ckpdir.resolve(part);
      
      OutputStream fos = Files.newOutputStream(this.path);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(ckp);
      oos.close();
    }
    
    
    private Checkpoint getCheckpoint() throws IOException
    {
      InputStream fis = Files.newInputStream(this.path);
      ObjectInputStream ois = new ObjectInputStream(fis);
      Object o;
      try {
        o = ois.readObject();
      } catch (ClassNotFoundException e) {
        o = null;
      }
      ois.close();
      return (Checkpoint)o;
    }
  }

  
  public UUID getLatestCheckpointId()
  {
    if (checkpointList.size() == 0)
      return null;
    return checkpointList.get(checkpointList.size() - 1);
  }
  
  
  public void addNewCheckpoint(UUID id)
  {
    checkpointList.add(id);
    checkpoints.put(id, new HashMap<>());
  }
  
  
  public boolean containsCheckpoint(UUID id)
  {
    return checkpoints.containsKey(id);
  }
  
  
  public void addCheckpointPart(UUID ckpId, Checkpoint ckpPart)
  {
    Map<Integer, CheckpointPartProxy> ckp = checkpoints.get(ckpId);
    CheckpointPartProxy cpp;
    try {
      cpp = new CheckpointPartProxy(ckpPart);
    } catch (IOException e) {
      return;
    }
    ckp.put(ckpPart.getComputeNode().getID(), cpp);
  }
  
  
  public Checkpoint getCheckpointPart(UUID ckpId, int logNodeId)
  {
    Map<Integer, CheckpointPartProxy> ckp = checkpoints.get(ckpId);
    CheckpointPartProxy cpp = ckp.get(logNodeId);
    if (cpp == null)
      return null;
    try {
      return cpp.getCheckpoint();
    } catch (IOException e) {
      return null;
    }
  }
  
  
  public enum ValidationResult
  {
    /** Some nodes do not belong in the checkpoint, some nodes appear twice, or a sink nodes
     * has been checkpointed. */
    INVALID,
    /** Only nodes in the dag have been captured in the checkpoint */
    VALID,
    /** Valid and every node in the dag is in the checkpoint */
    COMPLETE
  }
  
  
  public ValidationResult validateCheckpoint(UUID ckpId, JobDag checkpointed)
  {
    Set<Integer> leftNodes = new HashSet<>(checkpointed.allNodeIds());
    Map<Integer, CheckpointPartProxy> ckp = checkpoints.get(ckpId);
    
    leftNodes.remove(checkpointed.getSinkNodeId());
    
    for (Map.Entry<Integer, CheckpointPartProxy> part: ckp.entrySet()) {
      Integer nid = part.getKey();
      if (!leftNodes.contains(nid))
        return ValidationResult.INVALID;
      leftNodes.remove(nid);
    }
    
    if (leftNodes.isEmpty())
      return ValidationResult.COMPLETE;
    return ValidationResult.VALID;
  }
  
  
  public UUID getLatestCompleteCheckpointId(JobDag checkpointed)
  {
    int i = checkpointList.size() - 1;
    while (i >= 0) {
      UUID thisckp = checkpointList.get(i);
      if (validateCheckpoint(thisckp, checkpointed) == ValidationResult.COMPLETE)
        return thisckp;
      i--;
    }
    return null;
  }
}
