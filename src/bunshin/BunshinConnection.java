package bunshin;


import bunshin.util.*;
import bunshin.messaging.BunshinDeserialize;
import bunshin.storage.*;

import rice.environment.Environment;
import rice.p2p.commonapi.*;

import rice.pastry.NodeHandle;
import rice.pastry.NodeIdFactory;
import rice.pastry.PastryNode;
import rice.pastry.PastryNodeFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.*;

import java.io.*;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.nio.channels.*;

/**
 *
 * This class wraps a connection to Bunshin substrate and is used by the application as its main
 * DHT substrate.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class BunshinConnection {
  
  // Loads pastry settings
  public static Environment env = new Environment();

  // the port to begin creating nodes on
  public static int PORT = 5019;

  // the port on the bootstrap to contact
  public static int BOOTSTRAP_PORT = 5019;

  // the host to boot the first node off of
  public static String BOOTSTRAP_HOST = "localhost";

  // Reference to the Pastry node
  private PastryNode node;
  
  protected BunshinImpl bunshinApp;

  protected Endpoint endPoint;
  
  //boot
  protected InetSocketAddress bootaddress;

  

  // ----- ATTEMPT TO LOAD LOCAL HOSTNAME -----
  static {
    try {
      BOOTSTRAP_HOST = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
    }
  }

  /**
   * Creates a node and stablishs the connection with the network
   * @param String host
   * @param int port   
   *
   */
  public BunshinConnection(String bootHost, int bootPort) {
    try {
		createNode(bootHost,bootPort);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  /**
   * Creates a node and stablishs the connection with the network
   * @param Properties bunshin file
   */
  public BunshinConnection(Properties prop) throws Exception {

      String bootHost =(String)prop.get(Context.HOST);
      if (bootHost==null) bootHost = "auto";

      int bootPort = Integer.parseInt((String)prop.get(Context.PORT));

      createNode(bootHost,bootPort);
  }

  /**
   * Creates a node and stablishs the connection with the network
   * @param String of the path of the properties bunshin file
   */
  public BunshinConnection(String path) throws Exception {

    Properties prop = Utilities.loadProp(path);

    String bootHost =(String)prop.get(Context.HOST);
    if (bootHost==null) bootHost = "auto";

    int bootPort = Integer.parseInt((String)prop.get(Context.PORT));

    createNode(bootHost,bootPort);
  }

  /**
   * This method creates a new node which will serve as the underlying p2p layer
   *
   * @param args Constructor arguments: 0 - String bootstrap host; 1 - int bootstrap port; 2 - PastryNodeFactory factory; 3 - int protocol
 * @throws Exception 
   */
  public void createNode (String bootHost, int bootPort) throws Exception {

	 	    // disable the UPnP setting (in case you are testing this on a NATted LAN)
	    env.getParameters().setString("nat_search_policy","never");

	    
    if (!bootHost.equalsIgnoreCase ("auto")) {
      BOOTSTRAP_HOST = bootHost;
    }
    BOOTSTRAP_PORT = bootPort;
    PORT = bootPort;
 
    //  Generate the NodeIds Randomly
    NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

    PastryNodeFactory factory = null;
    //PastryNodeFactory factory = new DirectPastryNodeFactory (new RandomNodeIdFactory(env), new SphereNetwork(env), env);
        
    int newPort = changeBoundPort(BOOTSTRAP_HOST, BOOTSTRAP_PORT);
    
    while(node==null) {
    	
      //construct the PastryNodeFactory, this is how we use rice.pastry.socket
      factory = new SocketPastryNodeFactory(nidFactory, newPort, env);
      
      // Get bootstrap reference
      bootaddress = new InetSocketAddress (BOOTSTRAP_HOST, BOOTSTRAP_PORT);     

      // This will return null if we there is no node at that location
      //NodeHandle bootHandle = ((SocketPastryNodeFactory) factory).getNodeHandle(bootaddress);
      try {
        // construct a node, passing the null boothandle on the first loop will cause the node to start its own ring
        //node = factory.newNode((NodeHandle) bootHandle);
    	
    	// construct a node, but this does not cause it to boot
      	node = factory.newNode();
      	  
    	// in later tutorials, we will register applications before calling boot
        //node.boot(bootaddress);
    	  
      } catch(Exception e) {
    	newPort+=100;  
        System.out.println ("Port " + (newPort - 100) + " already bound. Trying " + newPort + "...");
      }
      
    }
    
    
  }
  

  /**
   * This method boots node previously created
   * 
 * @throws Exception 
   */
  public void bootNode () throws Exception {
    
    node.boot(bootaddress);
   
    // the node may require sending several messages to fully boot into the ring
    synchronized(node) {
       while(!node.isReady() && !node.joinFailed()) {
       // delay so we don't busy-wait
       node.wait(500);
            
       // abort if can't join
       if (node.joinFailed()) {
          throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason()); 
       }
     }       
    }
    
    System.out.println("Finished creating new node "+node);
    
    // wait 10 seconds
    env.getTimeSource().sleep(3000);
  } 

  
  private int changeBoundPort(String boothost, int bootport) throws IOException{

	  int newPort = bootport;
	  		  
	// If working in remote mode, check if port is already bound 
      while (true) {
    	ServerSocketChannel channel = null;        
		try {
    	    // Create a new non-blocking server socket channel
    		channel = ServerSocketChannel.open();
    		channel.configureBlocking(false);	    		    		
    		InetSocketAddress isa = new InetSocketAddress(boothost, newPort);    		
    		channel.socket().bind(isa);
    		
          channel.socket().close();
          channel.close();
          break;
        } catch (BindException e) {
          if (e.getMessage().contains("Address already in use")) {
            newPort += 100;
            System.out.println ("Port " + (newPort - 100) + " already bound. Trying " + newPort + "...");
          }
          else break;
        }
        finally {	          
          try {
        	channel.socket().close();
            channel.close();
          }
          catch (IOException ex) {}
        }
      }      
      if (bootport!=newPort) System.out.println("Port changed: "+bootport+" --> "+newPort);
      
	    return newPort;
     }
  

/**
   * Initializes the Bunshin layer.
   *
   * @param String id of the application
   * @param StorageManager the object manager
   * @param int replicaFactor indicates the number of copies in the network
   * @param boolean activate cache mode
   * @param boolean activate debug mode
   */
  public void init(String id, StorageManager manager, int replicaFactor, boolean cache, boolean debug) throws Exception {
	BunshinDeserialize bd = new BunshinDeserialize();
  	bunshinApp = new BunshinImpl(id, bd);
    bunshinApp.setStorageManager(manager);
    bunshinApp.setReplicationFactor(replicaFactor);
    if (cache) bunshinApp.activateCache();
    if (debug) bunshinApp.activateDebug();
    //We are only going to use one instance of this application on each PastryNode
    endPoint = node.buildEndpoint(bunshinApp,id);
    endPoint.setDeserializer(bd);
    
   
    // now we can receive messages
    endPoint.register();
    bunshinApp.setEndPoint (endPoint);
    
    bootNode();
    
    bunshinApp.setRefreshTime(20000);
    
  }	
  	
    /**
   * Initializes the Bunshin layer.
   *
   * @param String id of the application
   * @param StorageManager the object manager
   * @param int replicaFactor indicates the number of copies in the network
   * @param int refresh time period in seconds 
   * @param boolean activate cache mode
   * @param boolean activate debug mode
   */	  	
  public void init(String id, StorageManager manager, int replicaFactor, int time_sec, boolean cache, boolean debug) throws Exception { 	
	BunshinDeserialize bd = new BunshinDeserialize();
	bunshinApp = new BunshinImpl(id,bd);
    bunshinApp.setStorageManager(manager);
    bunshinApp.setReplicationFactor(replicaFactor);
    if (cache) bunshinApp.activateCache();
    if (debug) bunshinApp.activateDebug();
        
    //We are only going to use one instance of this application on each PastryNode
    endPoint = node.buildEndpoint(bunshinApp,id);
    endPoint.setDeserializer(bd);
   
    // now we can receive messages
    endPoint.register();
    bunshinApp.setEndPoint (endPoint);
    
    bootNode();
    
    bunshinApp.setRefreshTime(time_sec*1000);
    
  }
  /**
   * Initializes the Bunshin layer.
   *
   * @param String path of the bunshin properties file
   */
  public void init(Properties prop) throws Exception{

  	String id =(String)prop.get(Context.ID_APPLICATION);
  	StorageManager manager = (StorageManager)Class.forName((String)prop.get(Context.STORAGE_MANAGER)).newInstance();
  	manager.init(prop);
  	int replicaFactor = Integer.parseInt((String)prop.get(Context.REPLICA_FACTOR));
  	boolean cache = Context.TRUE.equals((String)prop.get(Context.CACHE));
  	boolean debug = Context.TRUE.equals((String)prop.get(Context.DEBUG));

    int time = Integer.parseInt((String)prop.get(Context.REFRESH_TIME));
  	init(id,manager,replicaFactor,time,cache,debug);
  	
  }
  
  /**
   * Initializes the Bunshin layer.
   *
   * @param String path of the bunshin properties file
   */
  public void init(String filename) throws Exception{

  	Properties prop = Utilities.loadProp(filename);
    init(prop);  	
  }
  /**
  * Obtains the Bunshin's application reference.
  * @return bunshinApp
  */
  public Bunshin getBunshinApp() {
	  return bunshinApp;
  }
  
  /**
   * Asynchronous get method
   * @param String value
   * @param BunshinGetClient the callback client
   */
  public void retrieveObject(String context, Id key, bunshin.listeners.BunshinGetClient client) throws IOException {
  	bunshinApp.get(context,key,client);
  }


  /**
   * Puts the pair (key,value)
   * @param Id of the key
   * @param Serializable value
   */
  public void storeObject(Id id, Serializable value) {
    bunshinApp.put(id,value);
  }
  
	/**
	 * Puts the pair (key,value)
	 * @param Context string
	 * @param Id of the key
	 * @param Serializable value
	 */
	public void storeObject(String context,Id id, Serializable value)
	{
		bunshinApp.put(context,id,value);
	}
  /**
   * Puts the pair (key,value), when key = value
   * @param String of the key
   * @param Serializable value
   * @return Id of the key
   */
  public Id storeObject(String key, Serializable value) {
  	Id id = Utilities.generateHash(key);
    bunshinApp.put(id,value);
    return id;
  }
  
    /**
   * Synchronous get method
   * @param Id of the key
   * @return Object value of this id key
   */
  
  private class BunshinGetClient implements bunshin.listeners.BunshinGetClient
  {
    public boolean lookupResultArrived = false;
    public Serializable value = null;    

	public void get (Serializable result) {

	    synchronized (this) {
            value = result;
            lookupResultArrived = true;
        }
    }
  }

    public Serializable retrieveObject(Id key) throws IOException {		
		return retrieveObject(Context.DEFAULT_CONTEXT, key);
	}
	  
   	/**
	 * Synchronous get method
	 * @param Context String
	 * @param Id of the key
	 * @return Serializable value of this id key
	 */
	public Serializable retrieveObject(String context,Id key) throws IOException {

		int timeout = 0;

        BunshinGetClient client = new BunshinGetClient();

		bunshinApp.get(context,key,client);

		synchronized (client)
		{
			while (!client.lookupResultArrived && timeout < 30)
			{
				try
				{
					client.wait (100);
				}
				catch (InterruptedException ex)
				{
				}
				timeout++;
			}
		}

		return client.value;
	}




	  /**
	   * Remove method
	   * @param String of the key
	   */
	  public void removeObject(String key) throws IOException {
	  	Id id = Utilities.generateHash(key);
	  	bunshinApp.remove(id);
	  }
	  
	  /**
	   * Remove method
	   * @param String of the key
	   */
	  public void removeObject(String context, String key) throws IOException {
	  	Id id = Utilities.generateHash(key);
	  	bunshinApp.remove(context, id);
	  }

  /**
   * Remove method
   * @param Id of the key
   */
  public void removeObject(Id idValue) throws IOException {
  	bunshinApp.remove(idValue);
  }
  
  /**
   * Remove method
   * @param Id of the key
   */
  public void removeObject(String context, Id idValue) throws IOException {
  	bunshinApp.remove(context, idValue);
  }
 
  /**
   * Leaves the bunshin layer
   */
  public void leave() {
  	bunshinApp.leave();
  }




}
