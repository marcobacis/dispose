package dispose.test;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;
import org.junit.internal.builders.NullBuilder;

import dispose.net.common.DataAtom;
import dispose.net.common.types.*;
import dispose.net.node.AtomsCache;

public class AtomsCacheTest
{
  
  @Test
  public void testPush() {
    
    testPushWithIndex(10, -1);
    testPushWithIndex(10, 5);
    testPushWithIndex(10, 9);
    testPushWithIndex(10, 10);
    
  }
  
  private void testPushWithIndex(int size, int idx) {
    AtomsCache cache = new AtomsCache();
    List<DataAtom> expected = new ArrayList<>();
    DataAtom acked = new NullData(); 
    
    for(int i = 0; i < size; i++) {
      DataAtom toPush = new FloatData((double) i);
      
      cache.push(toPush);

      if(i > idx) expected.add(toPush);
      else if (i == idx) acked = toPush;
    }
    
    if(idx >= size) acked = new NullData();
    
    cache.acked(acked);
    
    List<DataAtom> actual = cache.getAtoms();
    
    assertEquals(expected.size(), actual.size());
    
    for(int j = 0; j < expected.size(); j++) {
      assertEquals(expected.get(j), actual.get(j));
    }
    
  }
  
}
