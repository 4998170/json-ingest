package us.yuxin.sa.transformer;

public class TransformerFactory {
  public static Transformer get(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    return (Transformer)Class.forName(className).newInstance();
  }
}
