package dispose.net.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import dispose.net.message.Message;

public abstract class DataAtom extends Message
{
  private static final long serialVersionUID = -7832345233584092879L;
  private long timestamp;
  
  
  public DataAtom(long timestamp)
  {
    this.timestamp = timestamp;
  }
  
  
  public long getTimestamp()
  {
    return timestamp;
  }
  
  
  public UUID getUUID()
  {
    byte[] classn = this.getClass().getName().getBytes(StandardCharsets.UTF_8);
    ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES + classn.length);
    bytes.put(classn, 0, classn.length);
    bytes.putLong(classn.length, timestamp);
    return UUID.nameUUIDFromBytes(bytes.array());
  }
}
