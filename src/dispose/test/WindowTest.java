package dispose.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import dispose.net.common.DataAtom;
import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;
import dispose.net.node.operators.Window;

public class WindowTest
{

  @Test
  public void testIsFull()
  {
    Window win = new Window(5, 1);
    assertFalse(win.isFull());
    win.push(new FloatData(0));
    assertFalse(win.isFull());
    win.push(new FloatData(0));
    assertFalse(win.isFull());
    win.push(new FloatData(0));
    assertFalse(win.isFull());
    win.push(new FloatData(0));
    assertFalse(win.isFull());
    win.push(new NullData());
    assertFalse(win.isFull());
    win.push(new FloatData(0));
    assertTrue(win.isFull());
    win.push(new FloatData(0));
    assertTrue(win.isFull());
  }

  @Test
  public void testIsEmpty()
  {
    Window win = new Window(5, 1);
    assert(win.isEmpty());
    
    win.push(new NullData());
    assert(win.isEmpty());
    
    win.push(new FloatData(0));
    assertFalse(win.isEmpty());
    win.push(new FloatData(0));
    assertFalse(win.isEmpty());
    win.push(new FloatData(0));
    assertFalse(win.isEmpty());
    win.push(new FloatData(0));
    assertFalse(win.isEmpty());
    
    win.reset();
    assertTrue(win.isEmpty());
  }
  
  @Test
  public void testReset() {
    Window win = new Window(5, 1);
    
    assertTrue(win.isEmpty());
    
    for (int i = 0; i < 6; i++) {
      win.push(new FloatData(Math.random()));
    }
    
    assertTrue(win.isFull());
    win.reset();
    assertTrue(win.isEmpty());
  }
  
  @Test
  public void testWindowElements() {
    checkWindowWithParams(3, 1, 150);
    checkWindowWithParams(1, 1, 6);
    checkWindowWithParams(2, 1, 100);
    checkWindowWithParams(2, 2, 200);
    checkWindowWithParams(5, 1, 100);
    checkWindowWithParams(3, 1, 300);
    checkWindowWithParams(3, 3, 300);
    checkWindowWithParams(1, 2, 200);
    checkWindowWithParams(3, 3, 3);
    checkWindowWithParams(5, 7, 1000);
  }
  
  private void checkWindowWithParams(int size, int slide, int length) {
    checkWindowWithParams(size, slide, length, true);
  }
  
  private void checkWindowWithParams(int size, int slide, int length, boolean nulls) {
    List<DataAtom> atoms = new ArrayList<>(length);
    
    for (int i = 0; i < length; i++) {
      atoms.add(new FloatData(Math.random()));
    }

    List<List<DataAtom>> result = new ArrayList<>();
    List<List<DataAtom>> trusted = new ArrayList<>();

    Window win = new Window(size, slide);

    for (DataAtom a : atoms) {
      win.push(a);
      
      if (nulls && (Math.random()*100 < 20))
        win.push(new NullData());
      
      if(win.ready()) {
        win.move();
        result.add(win.getElements());
      }
    }
    

    // proper results
    for(int w = 0; (w+size-1) < length; w += slide) {
      List<DataAtom> elems = new ArrayList<>();
      for(int i = 0; i < size && w+i<length; i++)
        elems.add(atoms.get(w + i));
      
      trusted.add(elems);
    }
        
    assertEquals(trusted.size(), result.size());
    
    int reslength = result.size();
    
    for(int i = 0; i < reslength; i++) {
      List<DataAtom> resElems = result.get(i);
      List<DataAtom> trustElems = trusted.get(i);
      
      assertEquals(trustElems.size(), resElems.size());
            
      for(int e = 0; e < resElems.size(); e++) {
        DataAtom res = resElems.get(e);
        DataAtom trust = trustElems.get(e);
        assertEquals(trust.getClass(), res.getClass());
        if(res instanceof FloatData)
          assertEquals(trust, res);
      }
    }
  }

  
}
