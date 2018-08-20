package dispose.net.supervisor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import dispose.net.node.OperatorCheckpoint;

public class CheckpointArchive
{
  private List<UUID> checkpointList = new ArrayList<>();
  private Map<UUID, Map<Integer, OperatorCheckpoint>> checkpoints = new HashMap<>();

  
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
  
  
  public void addCheckpointPart(UUID ckpId, OperatorCheckpoint ckpPart)
  {
    Map<Integer, OperatorCheckpoint> ckp = checkpoints.get(ckpId);
    ckp.put(ckpPart.getOperator().getID(), ckpPart);
  }
  
  
  public OperatorCheckpoint getCheckpointPart(UUID ckpId, int logNodeId)
  {
    Map<Integer, OperatorCheckpoint> ckp = checkpoints.get(ckpId);
    return ckp.get(logNodeId);
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
    Map<Integer, OperatorCheckpoint> ckp = checkpoints.get(ckpId);
    
    leftNodes.remove(checkpointed.getSinkNodeId());
    
    for (Map.Entry<Integer, OperatorCheckpoint> part: ckp.entrySet()) {
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
