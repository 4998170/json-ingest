package us.yuxin.sa.transformer;

import java.util.Map;
import java.util.Properties;

public interface Transformer {
  public Map<String, Object> transform(Map<String, Object> data, Map<String, Object> params);

  public void setup(Properties conf);
  public void start();
  public void stop();
}