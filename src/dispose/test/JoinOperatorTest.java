package dispose.test;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;

import dispose.net.common.*;
import dispose.net.common.types.*;
import dispose.net.node.operators.JoinOperator;

public class JoinOperatorTest
{
  
  @Test
  public void testJoin() {
    
    testJoin(1, atomList(1.0, 2.0, 3.0, 5.0, 5.0, 0.0, 6.0),
                atomList(2.0, 1.0, 4.0, 5.0, 0.0, 3.0, 6.0, 0.0, 6.0),
                atomList(5.0, 5.0, 6.0, 6.0));
    
    testJoin(2, atomList(1.0, 2.0, 2.0, 0.0, 5.0, 0.0, 6.0),
                atomList(2.0, 1.0, 0.0, 5.0, 0.0, 3.0, 6.0, 7.0, 6.0),
                atomList(1.0, 2.0, 2.0, 5.0, 6.0, 6.0));
    
    testJoin(3, atomList(1.0, 2.0, 3.0, 4.0, 5.0, 0.0, 6.0, 7.0),
                atomList(2.0, 1.0, 4.0, 5.0, 0.0, 3.0, 0.0, 9.0, 6.0),
                atomList(1.0, 2.0, 4.0, 5.0, 3.0, 6.0));
    
  }
  
  public void testJoin(int size, List<DataAtom> left, List<DataAtom> right, List<DataAtom> testResult) {
    
    JoinOperator joinOp = new JoinOperator(0, size, 1, 2);
    
    int leftSize = left.size();
    int rightSize = right.size();
    int totSize = Math.max(leftSize, rightSize);
    
    List<DataAtom> result = new LinkedList<>();
    
    for(int i = 0; i < totSize; i++) {
      DataAtom leftAtom = i >= left.size() ? new NullData(-1) : left.get(i);
      DataAtom rightAtom = i >= right.size() ? new NullData(-1) : right.get(i);
      
      result.addAll(joinOp.processAtom(leftAtom, rightAtom));
    }
    
    assertEquals(testResult.size(), result.size());
    
    for(int r = 0; r < testResult.size(); r++) {
      assertEquals(testResult.get(r), result.get(r));
    }
    
  }
  
  private List<DataAtom> atomList(Double... atoms) {
    List<DataAtom> result = new LinkedList<>();
    
    for(Double val : atoms) {
      if(val == 0)
        result.add(new NullData(-1));
      else
        result.add(new FloatData(-1, val));
    }
    
    return result;
  }
}
