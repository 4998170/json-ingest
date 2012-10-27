package us.yuxin.sa.transformer;

import java.util.Map;

public interface Transformer {
  public Map<String, Object> transform(Map<String, Object> data, Map<String, Object> params);

  public void start();
  public void stop();
}