
package dispose.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;
import dispose.net.node.operators.MaxWindowOperator;

/**
 * Test for single-input operators (avg, min, max, sum etc... all but join) *
 */
public class WindowOperatorTest
{

  @Test
  public void testProcessAtom()
  {
    checkResultWithParams(3, 1, 150);
    checkResultWithParams(1, 1, 6);
    checkResultWithParams(2, 1, 100);
    checkResultWithParams(2, 2, 200);
    checkResultWithParams(5, 1, 100);
    checkResultWithParams(3, 1, 300);
    checkResultWithParams(3, 3, 300);
    checkResultWithParams(1, 2, 200);
    checkResultWithParams(3, 3, 3);
    checkResultWithParams(5, 7, 1000);
  }

  @Test
  public void testReset()
  {
    MaxWindowOperator op = new MaxWindowOperator(1, 5, 1);
    
    List<DataAtom> atoms = createAtomList(6);
    
    for(DataAtom a : atoms) op.processAtom(a);
    
    op.reset();
    assert(op.clock() == 0);
  }


  private List<DataAtom> createAtomList(int size)
  {

    List<DataAtom> atoms = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      atoms.add(new FloatData(Math.random()));
    }

    return atoms;
  }


  private void checkResultWithParams(int size, int slide, int length)
  {

    List<DataAtom> atoms = createAtomList(length);

    List<DataAtom> result = new ArrayList<>();
    List<DataAtom> trusted = new ArrayList<>();

    MaxWindowOperator op = new MaxWindowOperator(1, size, slide);

    for (DataAtom a : atoms) {
      List<DataAtom> res = op.processAtom(a);
            
      assert(res.size() <= 1); //for now, waiting for (key,value) implementation maybe
      
      if(res.size() == 1) 
        result.add(res.get(0));
    }

    // proper results
    for (int w = 0; w < length-size+1; w++) {
      if (w % slide == 0) {
        double max = ((FloatData) atoms.get(w)).floatValue();
        for (int i = 1; i < size; i++) {
          double val = ((FloatData) atoms.get(w + i)).floatValue();
          max = val > max ? val : max;
        }
        trusted.add(new FloatData(max));
      }
    }
    
    assert(result.size() == trusted.size());
    
    int reslength = result.size();
    
    for(int i = 0; i < reslength; i++) {
      DataAtom resAtom = result.get(i);
      DataAtom trustAtom = trusted.get(i);
      if((resAtom instanceof FloatData) && (trustAtom instanceof FloatData)) {
          double res = ((FloatData) resAtom).floatValue();
          double max = ((FloatData) trustAtom).floatValue();
      
          assert(res == max);
      }
      
      assert(resAtom.getClass() == trustAtom.getClass());
    }
    
  }

}
