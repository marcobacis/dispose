package dispose.net.node;

import java.io.Serializable;
import java.util.*;

import dispose.net.common.DataAtom;

public class AtomsCache implements Serializable
{
  private static final long serialVersionUID = 480191479336858716L;
  
  private List<DataAtom> cachedAtoms = new LinkedList<>();
  
  /**
   * Insert the given atom into the cache
   * @param atom    The atom to be cached
   */
  public void push(DataAtom atom)
  {
    this.cachedAtoms.add(atom);
  }
  
  /**
   * @return the list of all the atoms cached
   */
  public List<DataAtom> getAtoms() {
    return new ArrayList<>(this.cachedAtoms);
  }
  
  /**
   * Removes from the cache all the atoms with a timestamp less than the ack's one
   * @param ack     The ACKed DataAtom
   */
  public void acked(DataAtom ack)
  {
    for(Iterator<DataAtom> iter = this.cachedAtoms.iterator(); iter.hasNext(); ) {
      DataAtom atom = iter.next();
      
      if(atom.getTimestamp() <= ack.getTimestamp()) iter.remove();
    } 
  }
  
}
