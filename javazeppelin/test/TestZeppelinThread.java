import java.util.TreeMap;
import java.util.Map;
import net.qihoo.zeppelin.Zeppelin;
import net.qihoo.zeppelin.ZeppelinException;

public class TestZeppelinThread extends Thread {
  int pauseTime;
  public TestZeppelinThread(int pTime) {
    this.pauseTime = pTime;
  }


  public void run() {
    Zeppelin test = new Zeppelin("127.0.0.1", "9221", "test1"); 
    for (int i = 0; i < 10; ++i) {
      this.doIt(test);
      try {
        Thread.sleep(pauseTime * 100);
      } catch (Exception e) {
        e.printStackTrace();      
      }
    }
    try {
      test.removeZeppelin();
    } catch(ZeppelinException e) {
      e.printStackTrace();
    } 
  }

  public void doIt(Zeppelin test) {
    try {
      String key1 = "key1";
      String key2 = "key2";
      String key3 = "key3";
      String value1 = "value1";
      String value2 = "value2";
      String value3 = "value3";

      boolean res = test.set(key1, value1);
      System.out.println("thread" + pauseTime + " set " + key1 + " " + value1 + " "+ res);
      res = test.set(key2, value2);
      System.out.println("thread" + pauseTime + " set " + key2 + " " + value2 + " " + res);
      res = test.set(key3, value3);
      System.out.println("thread" + pauseTime + " set " + key3 + " " + value3 + " " + res);

      String value = test.get(key1);
      System.out.println("thread" + pauseTime + " get " + key1 + ": " + value);

      res = test.delete(key1);
      System.out.println("thread" + pauseTime + " delete " + key1 + ": " + res);
      res = test.delete("key9");
      System.out.println("thread" + pauseTime + " delete key9" + ": " + res);
      value = test.get(key1);
      System.out.println("thread" + pauseTime + " get " + key1 + ": " + value);

      String[] keys = {key2, "key6"};
      TreeMap<String, String> tm = test.mget(keys);
      for (Map.Entry m:tm.entrySet()) {
        System.out.println("thread" + pauseTime + " mget" + " " + m.getKey() + " " + m.getValue());
      }
    } catch(ZeppelinException e) {
      e.printStackTrace();
    }
  } 

  public static void main(String[] args) {
    for (int i = 1; i <= 100; i++) {
      TestZeppelinThread tqt = new TestZeppelinThread(i);
      tqt.start();
    }
  }

}


