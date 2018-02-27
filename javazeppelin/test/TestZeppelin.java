import java.util.TreeMap;
import java.util.Map;
import net.qihoo.zeppelin.Zeppelin;
import net.qihoo.zeppelin.ZeppelinException;

public class TestZeppelin {
  public static void main(String args[]) {
    Zeppelin test = new Zeppelin("127.0.0.1", "9221", "test1");
    try {
      String key1 = "key1";
      String key2 = "key2";
      String key3 = "key3";
      String value1 = "value1";
      String value2 = "value2";
      String value3 = "value3";

      boolean res = test.set(key1, value1);
      System.out.println("set " + key1 + " " + value1 + " "+ res);
      res = test.set(key2, value2);
      System.out.println("set " + key2 + " " + value2 + " " + res);
      res = test.set(key3, value3);
      System.out.println("set " + key3 + " " + value3 + " " + res);

      String value = test.get(key1);
      System.out.println("get " + key1 + ": " + value);

      res = test.delete(key1);
      System.out.println("delete " + key1 + ": " + res);
      res = test.delete("key9");
      System.out.println("delete key9" + ": " + res);
      value = test.get(key1);
      System.out.println("get " + key1 + ": " + value);

      String[] keys = {key2, "key6"};
      TreeMap<String, String> tm = test.mget(keys);
      for (Map.Entry m:tm.entrySet()) {
        System.out.println("mget" + " " + m.getKey() + " " + m.getValue());
      }
      test.removeZeppelin();
    } catch(ZeppelinException e) {
      e.printStackTrace();
    } 
  }
}
