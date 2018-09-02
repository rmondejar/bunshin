package bunshin;

import bunshin.listeners.*;
import bunshin.storage.*;

import java.io.Serializable;
import java.util.Hashtable;
import rice.p2p.commonapi.*;


/**
 *
 * This interface is exported by all instances of Bunshin DHT. 
 * 
 * @author Ruben Mondejar
 */
public interface Bunshin {

  /**
   * Stores the (key,value) pair in the Bunshin DHT in the default context and with default field
   *
   * @param key Id of the value
   * @param value Serializable to store
   */
  public void put(Id key, Serializable value);
 
  /**
   * Stores the (context,key,value) in the Bunshin DHT with default field
   *
   * @param context Id of the context
   * @param key Id of the value
   * @param value Object to store
   */  
  public void put(String context, Id key, Serializable value);  
  
  /**
   * Stores the (key,value) pair in the Bunshin DHT in a default field
   *
   * @param key Id of the value
   * @param value Object to store
   * @param client listener BunshinPutClient
   */
  public void put(Id key, Serializable value, BunshinPutClient client);
  
   /**
   * Stores the (context,key,value) in the Bunshin DHT with result listener
   *
   * @param context Id of the context
   * @param key Id of the value
   * @param value Object to store
   * @param client listener BunshinPutClient
   */  
  
  public void put(String context, Id key, Serializable value, BunshinPutClient client);

   /**
   * Removes the key/value of the Bunshin DHT 
   *
   * @param key Id of the value   
   */  
  public void remove(Id key); 

  /**
   * Removes the (context,key)/value of the Bunshin DHT 
   *
   * @param context Id of the context
   * @param key Id of the value   
   */  
  public void remove(String context, Id key);
     
  /**
   * Removes the key/value of the Bunshin DHT 
   *
   * @param key Id of the value
   * @param client listener BunshinRemoveClient   
   */  
  public void remove(Id key, BunshinRemoveClient client);

   /**
   * Removes the (context,key)/value of the Bunshin DHT 
   *
   * @param context Id of the context
   * @param key Id of the value   
   * @param client listener BunshinRemoveClient
   */  
  public void remove(String context, Id key, BunshinRemoveClient client);

  /**
   * Returns the value of the specific key if the values is stored in the local node
   *
   * @param key Id of the value   
   */   
  public Serializable getLocal(Id key);
    
  /**
   * Returns the value of the specific key if the values is stored in the local node
   *
   * @param context Id of the context
   * @param key Id of the value
   * 
   */   
  public Serializable getLocal(String context, Id key);
    
  /**
   * Returns the value of the specific key using the Client application 
   *
   * @param key Id of the value 
   * @param client listener that implements BunshinClient     
   */   
  public void get(Id key, BunshinGetClient client);
    
  /**
   * Returns the values of the specific key and sets the Client application 
   *
   * @param context Id of the context
   * @param key Id of the value
   * @param client listener that implements BunshinClient     
   */  
  public void get(String context,Id key, BunshinGetClient client);
  
  /**
   * Before leaving the network, the application must call to this method
   */
  public void leave();
  
  /**
   * Sets the number of replicas  
   *
   * @param the number of replicas 
   */
  public void setReplicationFactor(int replicas);
  
  /**
   * Sets the storage manager
   *
   * @param storage manager
   */  
   public void setStorageManager(StorageManager storage);
 	
  /**
   * Returns the number of replicas used 
   *
   * @return the number of replicas 
   */
  public int getReplicationFactor();
  
  public void setRefreshTime(int time);
  
  public void setEndPoint (Endpoint ep);
   
  public StorageManager getStorageManager();
  
  public void activateCache();
  
  public void activateDebug();
  
  public Hashtable getStorageInfo();
  
  public Hashtable getReplicaInfo();
  
   /**
   * Activates the information of a specific context
   *   
   * @param path String path of the storage directory (DiskStorage and XMLStorage) 
   */
    public void createContext(String context, BunshinMergeClient bmc);


  }

