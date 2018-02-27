// Copyright 2017 Qihoo
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http:// www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.qihoo.zeppelin;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.TreeMap;
import net.qihoo.zeppelin.ZeppelinException;

public class Zeppelin {
  static {
    try{
      Zeppelin.loadLib();
    } catch(IOException e) {
      System.out.println("load qconf library failed");
      System.exit(1);
    }
  }
  public Zeppelin(String ip, String port, String table) {
    try{
      createZeppelin(ip, port, table);
    } catch(ZeppelinException e) {
      e.printStackTrace();
      System.out.println("init zeppelin failed"); 
      System.exit(1);
    }
  } 
  private long ptr = 0;
  /**
   * create zeppelin client instance
   * 
   * @param ip meta ip
   * @param port meta port
   * @param table table name
   * @exception ZeppelinException
   */
  private native void createZeppelin(String ip, String port, String table) throws ZeppelinException;
  /**
   * delete zeppelin client instance
   * @exception ZeppelinException
   */
  public native void removeZeppelin() throws ZeppelinException;
  /**
   * set key value through initialized zeppelin instance
   *
   * @param key key
   * @param value value 
   * @return if set success
   * @exception ZeppelinException
   */
  public native boolean set(String key, String value) throws ZeppelinException;  
  /**
   * get key value through initialized zeppelin instance
   *
   * @param key key
   * @return value according to the corresponding key
   * @exception ZeppelinException
   */
  public native String get(String key) throws ZeppelinException;
  /**
   * get multiple keys' values
   *
   * @param keys string array of keys
   * @return a map of keys and values
   * @exception ZeppelinException
   */
  public native TreeMap<String, String> mget(String[] keys) throws ZeppelinException;
  /**
   * delete key 
   * 
   * @param key key value
   * @return if delete success
   * @exception ZeppelinException
   */
  public native boolean delete(String key) throws ZeppelinException;

  private static void exceptionCallback(String errmsg) throws ZeppelinException {
    throw new ZeppelinException(errmsg); 
  }
    
  private synchronized static void loadLib() throws IOException {
    String libFullName = "libzeppelin.so";
    String tmpLibFilePath = System.getProperty("java.io.tmpdir") + File.separator + libFullName;
    InputStream in = null;
    BufferedInputStream reader = null;
    FileOutputStream writer = null;

    File extractedLibFile = File.createTempFile("libzeppelin",".so");
    try {
      in = Zeppelin.class.getResourceAsStream(File.separator + libFullName);
      Zeppelin.class.getResource(libFullName);
      reader = new BufferedInputStream(in);
      writer = new FileOutputStream(extractedLibFile);
      byte[] buffer = new byte[1024];
      while (reader.read(buffer) > 0) {
        writer.write(buffer);
        buffer = new byte[1024];
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(in!=null)
        in.close();
      if(writer!=null)
        writer.close();
    }
    System.load(extractedLibFile.toString());
    if (extractedLibFile.exists()) {
      extractedLibFile.delete();
    }
  }
}
