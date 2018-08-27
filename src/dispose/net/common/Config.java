package dispose.net.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Config
{
  public static final int nodeCtrlPort = 8000;
  public static final int nodeOperatorPort = 8001;
  
  public static final int minSourceThrottle = 100;
  
  public static final int heartbeatPeriod = 5000;
  public static final int checkpointPeriod = 10000;
  
  public static final int recoveryRetryPeriod = 5000;
  
  public static final Path checkpointDataRoot = Paths.get(System.getProperty("user.home"), ".dispose");
}
