package dispose.test;

import static org.junit.Assert.*;


import org.junit.Test;

import dispose.net.common.types.FloatData;
import dispose.net.common.types.NullData;
import dispose.net.node.operators.Window;

public class WindowTest
{

  @Test
  public void testIsFull()
  {
    Window win = new Window(5, 1);
    assert(!win.isFull());
    win.push(new FloatData(0));
    assert(!win.isFull());
    win.push(new FloatData(0));
    assert(!win.isFull());
    win.push(new FloatData(0));
    assert(!win.isFull());
    win.push(new FloatData(0));
    assert(!win.isFull());
    win.push(new NullData());
    assert(!win.isFull());
    win.push(new FloatData(0));
    assert(win.isFull());
    win.push(new FloatData(0));
    assert(win.isFull());
  }

  @Test
  public void testIsEmpty()
  {
    Window win = new Window(5, 1);
    assert(win.isEmpty());
    
    win.push(new NullData());
    assert(win.isEmpty());
    
    win.push(new FloatData(0));
    assert(!win.isEmpty());
    win.push(new FloatData(0));
    assert(!win.isEmpty());
    win.push(new FloatData(0));
    assert(!win.isEmpty());
    win.push(new FloatData(0));
    assert(!win.isEmpty());
    
    win.reset();
    assert(win.isEmpty());
  }
  
  @Test
  public void testReset() {
    Window win = new Window(5, 1);
    
    assert(win.isEmpty());
    
    for (int i = 0; i < 6; i++) {
      win.push(new FloatData(Math.random()));
    }
    
    assert(win.isFull());
    win.reset();
    assert(win.isEmpty());
  }

  
}
