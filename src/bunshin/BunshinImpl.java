/*******************************************************************************
 * Bunshin : DHT Replication & Caching
 * Copyright (C) 2004-2005 Ruben Mondejar
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 ******************************************************************************/

package bunshin;

import bunshin.util.*;
import bunshin.storage.*;
import bunshin.listeners.*;
import bunshin.messaging.*;

import rice.p2p.commonapi.*;

import java.util.*;
import java.io.*;

/**
 * 
 * The provided implementation of Bunshin DHT.
 * 
 * @author <a href="mailto: ruben.mondejar@urv.net">Ruben Mondejar </a>
 * 
 */

public class BunshinImpl implements Bunshin, Application {
   
	private boolean first = true;

	// ini params
	private int time = 20000;

	private final int MAX_REQUEST_TIME = 30;

	private final int TTL_CACHE = 50;
	private final int MAX_CACHE = 16;

	private int replicationFactor = 1;
	
	// My data
	private StorageManager storage;

	// Information about the nodes that replicate my data
	private ReplicaTable replicaNHs = new ReplicaTable();  // (context, key)
															// --> Collection NH
															// node/s with
															// replica/s

	// replicas from other nodes
	private Hashtable<NodeHandle,StorageManager> replicaBuckets = new Hashtable<NodeHandle,StorageManager>(); // Node
																												// NH
																												// -->
																												// StorageManager
																												// (context,key,value)
	private Hashtable<Id,NodeHandle> replicaKeyOwner = new Hashtable<Id,NodeHandle>();  // Key
																						// -->
																						// Node
																						// NH

	// Cache support
	// private Hashtable cache = new Hashtable(); //ini_version, later --> Cache
	// struct (fifo & temporal)
	private Cache cache = new Cache(TTL_CACHE,MAX_CACHE); // (fifo & temporal)

	private Hashtable<String, Hashtable<Id, PriorityList>> control = new Hashtable<String, Hashtable<Id, PriorityList>>();  // context ->
													// (key,PriorityList)
	

	private Endpoint endPoint = null;

	private PeriodicTask refreshTask = new PeriodicTask();
	
	private Hashtable<NodeHandle,Integer> status = new Hashtable<NodeHandle,Integer>();
	//int -> 0 alive, 1 dead, 2 unknown, 3 waiting

	private Timer timer = new Timer();

	private boolean overwrite = true;
	
	private boolean debug = false;

	private boolean caching = false;

	/**
	 * Identification of the application.
	 */
	public static String applicationId= "Bunshin";
	/**
	 * Identification of the application instance.
	 */
	private String appId = applicationId;
	
	private BunshinDeserialize bd;

	/**
	 * Bunshin Clients
	 */
	private Hashtable<Id, Vector<BunshinPutClient>> put_clients = new Hashtable<Id, Vector<BunshinPutClient>>();
	private Hashtable<Id, Vector<BunshinGetClient>> get_clients = new Hashtable<Id, Vector<BunshinGetClient>>();
	private Hashtable<Id, Vector<BunshinRemoveClient>> remove_clients = new Hashtable<Id, Vector<BunshinRemoveClient>>();
	private Hashtable<String, BunshinMergeClient> mergers = new Hashtable<String, BunshinMergeClient>();
	

	/**
	 * Constructor
	 */
	public BunshinImpl(){}

	public BunshinImpl(String name, BunshinDeserialize bd)	{
		this.appId = name;
		this.bd = bd;
	}

	public void setId(String name)	{
		this.appId = name;
	}

	/**
	 * Gets the identification of the Application
	 */
	public String getId()	{
		return appId;
	}


    public void setRefreshTime(int time) {
    	this.time = time;    	
    }
    
	public void setEndPoint (Endpoint ep)	{
		endPoint = ep;	
		if (first) timer.schedule(refreshTask,0,time);
	  else first=false;
		log("setEndPoint","I'm node "+endPoint.getLocalNodeHandle().getId());
	}
	
	public void setStorageManager(StorageManager storage) {
		this.storage = storage;
	}
	
	   /**
		 * Load the information of a specific context
		 * 
		 * @param context
		 *            Id of the context
         */
    public void createContext(String context, BunshinMergeClient bmc){    		
    	storage.loadContext(context);
    	if (bmc!=null) mergers.put(context, bmc);
    	refresh(0);
    	
    }


	public void activateDebug()	{
		this.debug=true;
		System.out.println("DEBUG ON");
	}

	public void activateCache()	{
		this.caching = true;
		System.out.println("CACHE ON");
	}

	public StorageManager getStorageManager() {
		return storage;
	}

	private void log(String from, String text)	{
		if (debug) System.out.println("DEBUG "+from+"> "+text);
	}

	/**
	 * Returns whether or not is the owner for the given key
	 * 
	 * @param key
	 *            The key in question
	 * @return Whether or not we are currently the root
	 */
	public boolean isOwner(Id key) {
		NodeHandleSet set = endPoint.replicaSet(key, replicationFactor);
		//System.out.println ("I'm the owner? ["+key+"] -> "+set);

		if (set==null)
			return false;
		else {
			
			Id x = endPoint.getId();

			NodeHandle nh = set.getHandle(0);
			if (nh==null) 	{
				return false;
			}
			return nh.getId().equals(x);
		}


	}

	/**
	 * Sends to the first node of the replicaSet a special message to check if
	 * that have the replicated keys. Remaining nodes are going to receive a
	 * normal message.
	 * 
	 */
	public void leave () {
        timer.cancel();    
	}
	
	
	/**
	 * ContextInfo = (Context, Bucket) Bucket = (Key, Values) Values = (Field,
	 * Object)
	 */
	 
	public Hashtable<String, Bucket> getStorageInfo() {

	   Hashtable<String, Bucket> buckets = storage.getBuckets();        	  
	   Hashtable<String, Bucket> nodeInfo = new Hashtable<String, Bucket>();        	       	      	
       
	   for (String context : buckets.keySet()) {
       
		 Bucket bucket = buckets.get(context);                 	  
         if (bucket.size()>0) nodeInfo.put(context,(Bucket) bucket.clone());        	  
       }      
       
       return nodeInfo;
		
	}
	
	/**
	 * StorageInfo = (NodeNH, ContextInfo) ContextInfo = (Context, Bucket)
	 * Bucket = (Key, Values) Values = (Field, Object)
	 */
	 
	public Hashtable<NodeHandle, Hashtable<String, Bucket>> getReplicaInfo() {

        Hashtable<NodeHandle, Hashtable<String, Bucket>> storageInfo = new Hashtable<NodeHandle, Hashtable<String, Bucket>>();
        
        for (NodeHandle nh : replicaBuckets.keySet()) {
        
        	StorageManager replicaNode = (StorageManager) replicaBuckets.get(nh);
        	Hashtable<String, Bucket> buckets = replicaNode.getBuckets();
        	  
            Hashtable<String, Bucket> nodeInfo = new Hashtable<String, Bucket>();
            for (String context: buckets.keySet()) {
        	
              Bucket bucket = buckets.get(context);        	  
        	  
        	  if (bucket.size()>0) nodeInfo.put(context,(Bucket) bucket.clone());        	  
        	}        	
        	if (nodeInfo.size()>0)  storageInfo.put(nh,nodeInfo);          	        	
        }
        return storageInfo;
		
	}	


	/**
	 * Sets the number of replicas
	 * 
	 * @param the
	 *            number of replicas
	 */
	public void setReplicationFactor(int replicas) {
		replicationFactor=replicas;
	}

	/**
	 * Returns the number of replicas used
	 * 
	 * @return the number of replicas
	 */
	public int getReplicationFactor()
	{
		return replicationFactor;
	}
	

	/**
	 * Checks that is the owner all keys are already
	 */
	private void checkMyKeys() {
				
		Hashtable<String, Bucket> buckets = storage.getBuckets();
		log ("checkMyKeys","Checking my keys : "+buckets.keySet());
				
		for (String context : buckets.keySet()) {
			Bucket bucket = storage.getBucket(context);
			for (Id key : bucket.keySet()) {
				if (!isOwner(key)) 	{
					
					log ("checkMyKeys","I'm not the owner of the key : ["+key+"]");
				
					    Serializable obj = storage.extract(context,key);
					    put(context,key,obj,new BunshinAutoPutClient(context,key));
						undoReplica(context,key);						
						// put the all values
						
				}
				else log ("checkMyKeys","I'm still the owner of the key : ["+key+"]");
			}
		}
	}

	private Collection<NodeHandle> remFailNH(Collection<NodeHandle> c) {

	    NodeHandle localNH = endPoint.getLocalNodeHandle();
		Collection<NodeHandle> c2 = new Vector<NodeHandle>();
		Iterator<NodeHandle> it2 = c.iterator();
		while (it2.hasNext()) {
			
			NodeHandle nh = (NodeHandle) it2.next();

			if (isAlive(nh) || nh.equals(localNH))  {
				c2.add(nh);
			}
		}
		return c2;
	}

	private void addReplica(String context,Id key, Collection<NodeHandle> c, int tries)	{

		NodeHandle localNH = endPoint.getLocalNodeHandle();
		Serializable values = storage.extract(context,key);
		Collection<NodeHandle> v = Utilities.convert(endPoint.replicaSet(key,c.size()+tries+1));
		v.remove(localNH);
		v.removeAll(c);		
		// System.out.println("NHs : "+v);
		
		for (NodeHandle nh : v) {
					
			if (nh!=null && !nh.equals(localNH)){
				
				// new replica try sended
				// endPoint.route(null,new
				// ReplicaMessage(key,values,localNH),nh);
				ReplicaMessage msg = new ReplicaMessage(key,values,localNH);
				msg.setContext(context);
				endPoint.route(null,msg,nh);
			}
		}
	}

	/**
	 * Checks the replica nodes that stores my keys/values
	 */
	private void checkMyRemoteReplicas() {
		
		Hashtable<String, Bucket> buckets = storage.getBuckets();
		for (String context : buckets.keySet()) {
			Bucket bucket = storage.getBucket(context);
		
		    for (Id key : bucket.keySet())	{
				
				Collection<NodeHandle> nhs = replicaNHs.get(context,key);
				
				if (nhs==null) nhs = new HashSet<NodeHandle>();
				else  nhs = new HashSet<NodeHandle>(nhs);
				
				replicaNHs.put(context,key,nhs);
								
				if (nhs!=null){
					
					// Check to find some failed node
					HashSet<NodeHandle> nhs1 = new HashSet<NodeHandle>(nhs); 
					Collection<NodeHandle> nhs2 = remFailNH((Collection<NodeHandle>) nhs1.clone());
					// Chech to find a not suficient number of copies of some
					// key
					 
					//System.out.println(key+" : "+nhs2.size()+" < "+replicationFactor+"?");				
										
					int tries = replicationFactor - nhs2.size();
					if (tries>0) {
					  //System.out.println("Try "+tries+" times to complete :"+nhs2);					
					  addReplica(context,key,nhs2,tries);					
					}
					
				}
			} //System.out.println("-------- "+size);
		}
	}

	private void checkReplicas() {

		Vector<NodeHandle> v = new Vector<NodeHandle>(replicaBuckets.keySet());
		log ("checkMyRemoteReplicas"," owners : "+v);
		for(int i=0; i<v.size();i++) {
			
			NodeHandle nh = (NodeHandle) v.get(i);
			
			if (nh.equals(endPoint.getLocalNodeHandle()) || !isAlive(nh))	{
				
				log ("checkMyRemoteReplicas"," cleaning owner : "+nh);
				
				// System.out.println("id "+endPoint.getId()+" checkReplica of "+nh+" is alive ?"+nh.isAlive());

				StorageManager replicas = (StorageManager) replicaBuckets.remove(nh);
                if (replicas!=null) {
				   Hashtable<String,Bucket> replicaBuckets = replicas.getBuckets();				   
				   for (String context : replicaBuckets.keySet()) {				
					
					 Bucket bucket = replicas.getBucket(context);
	                 if (bucket!=null) {
					   
	                   Vector<Id> c = new Vector<Id>(bucket.keySet());
					   while(!c.isEmpty())  {

						try {
						  Id key = c.firstElement();
						  log ("checkMyRemoteReplicas"," moving key : "+key);
						  Serializable obj = replicas.remove(context,key);
						  c.remove(key);
						  replicaKeyOwner.remove(key);
						  put(context, key, obj);                          
						}
						catch(StorageException ex) 	{
						  ex.printStackTrace();
						}
					  }
					}
				}
			  }
			}
			else if (debug) {

				StorageManager replicas = (StorageManager) replicaBuckets.get(nh);
                if (replicas!=null) {
				   Hashtable<String,Bucket> replicaBuckets = replicas.getBuckets();				   
				   for (String context : replicaBuckets.keySet()) {				
					
					 Bucket bucket = replicas.getBucket(context);
	                 if (bucket!=null) {					   
	                   for(Id key : bucket.keySet()) {
	                	   log ("checkReplicas","I'm still have the replica : ["+key+"]");
	                   }
	                 }  
				   }	
                }  
			}
			
			
		}
	}

	private boolean isAlive(NodeHandle nh) {		
		
		if (nh.equals(endPoint.getLocalNodeHandle())) return true;

		if (status.containsKey(nh)) {
			int state = status.get(nh);
			if (state==0) 
				return true;
			else if (state==1) 
				return false;
			else {
				status.put(nh,2);
				return endPoint.isAlive(nh);
			}
		}
		
		return endPoint.isAlive(nh);
	}

	/**
	 * This method is invoked to inform the application that the given node has
	 * either joined or left the neighbor set of the local node, as the set
	 * would be returned by the neighborSet call.
	 * 
	 * @param handle
	 *            The handle that has joined/left
	 * @param joined
	 *            Whether the node has joined or left
	 */
	public void update(NodeHandle handle, boolean joined) {

		
		if (joined)	{
			log ("update"," new node : "+handle);
			status.put(handle, 0);
			// only checkMyKeys();
			refresh(1);
			
		}
		else {
			log ("update"," old node : "+handle);
			status.put(handle, 1);
			// call to checkMyRemoteReplicas() & checkReplicas()
			refresh(2);
			
		}

	}

	/**
	 * This method is invoked on applications when the underlying node is about
	 * to forward the given message with the provided target to the specified
	 * next hop. Applications can change the contents of the message, specify a
	 * different nextHop (through re-routing), or completely terminate the
	 * message.
	 * 
	 * @param message
	 *            The message being sent, containing an internal message along
	 *            with a destination key and nodeHandle next hop.
	 * 
	 * @return Whether or not to forward the message further
	 */
	// public boolean forward(Message message) {
	// System.out.println ("Forward app ["+appId+"] node
	// ["+endPoint.getId()+"]");
	// return true;
	// }

	public boolean forward(RouteMessage message) {
		
		// System.out.println ("Forward app ["+appId+"] node
		// ["+endPoint.getId()+"]");
		// System.out.println ("RouteMessage, prev :
		// ["+message.getPrevNodeHandle()+"], next :
		// ["+message.getNextHopHandle()+"]");

		boolean forward = true;

		try {
		BunshinMessage msg = (BunshinMessage) message.getMessage(bd);
		String context = msg.getContext();

		if (caching && msg instanceof GetMessage) {
			
			GetMessage gmsg = (GetMessage) msg;
			if (gmsg.getPhase()==1) {

				Id key = gmsg.getKey();			    
				Serializable value = null;
				boolean haveValue = false;

				if (isOwner(key)) {
					
					value = storage.extract(context,key);					
					if (value!=null) haveValue = true;
				}

					// I'm not owner but I have replica
				else if (replicaKeyOwner.containsKey(key))	{
                   
  			        value = getReplicaValue(context,key);
  			    	 
					GetMessage fmsg = new GetMessage(key,value);
					fmsg.setContext(context);
					endPoint.route (null, fmsg, gmsg.getNH());

					forward = false;
					haveValue = true;
					log ("forward","Replica response "+key);
				}
					// I'm not owner and don't have any replica, but I have
					// value-cache
				else if (cache.contains(context,key))	{
					
				    value = cache.get(context,key); 			        
					 
					GetMessage fmsg = new GetMessage(key,value);
					fmsg.setContext(context);
					endPoint.route (null, fmsg, gmsg.getNH());

					forward = false;
					haveValue = true;
					// CACHE_LOG
					log ("forward","Cache response "+key);
				}

				if (haveValue)		{

					NodeHandle previous = gmsg.getPreviousNH();
					Id obj_key = gmsg.getKey();

					// CONTROL VERSION
					Hashtable<Id, PriorityList> pairs = control.get(context);
					if (pairs==null) pairs = new Hashtable<Id, PriorityList>();

					PriorityList list = (PriorityList) pairs.get(obj_key);
					if (list==null) list = new PriorityList();

					list.update(previous);

					pairs.put(obj_key,list);
					control.put(context,pairs);

					log ("forward","Request updated from "+previous.getId()+" key "+obj_key);

				}
				gmsg.setPreviousNH(endPoint.getLocalNodeHandle());
			}
		}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return forward;
	}

	/**
	 * This method is called on the application at the destination node for the
	 * given id.
	 * 
	 * @param id
	 *            The destination id of the message
	 * @param message
	 *            The message being sent
	 */
	public void deliver (Id id, Message message){

		Id key;
		Serializable value;
		boolean successful;

		BunshinMessage bm = (BunshinMessage) message;
		String context = bm.getContext();

		log ("deliver",message.toString());

		switch (bm.getType()) {
		
          case MessageType.PUT_MESSAGE:
		
			PutMessage putMessage = (PutMessage) message;
			key = putMessage.getKey();
			value = putMessage.getValue();
			NodeHandle originator = putMessage.getNH();
			            
			boolean putAck = putLocal(context,key,value,originator);
			
			PutAckMessage ackMessage = new PutAckMessage(key,putAck,endPoint.getLocalNodeHandle());
			ackMessage.setContext(context);
			endPoint.route (null, ackMessage, originator);
		
		  break;
	      case MessageType.PUT_ACK_MESSAGE:
		  
		    PutAckMessage putAckMessage = (PutAckMessage) message;
		    key = putAckMessage.getKey();	  
		    successful = putAckMessage.getSuccessful();
		 		  	
		    if (put_clients!=null) {
		    
		      // Client is only for one time
			  Vector<BunshinPutClient> id_clients = put_clients.remove(key);
			  if (id_clients!=null) {
			
			  for(int i=0; i<id_clients.size(); i++)	{
			    BunshinPutClient client = id_clients.get(i);
			    client.put(successful,putAckMessage.getSource());
			  }
			}
		  }	
	      
		  break;
          case MessageType.REMOVE_MESSAGE:
			
			RemoveMessage removeMessage = (RemoveMessage) message;
			key = removeMessage.getKey();
			
			try {
	        	
				// remove replicas
				undoReplica(context,key);
				log("deliver","Removing "+key);
				// value = storage.extract(context,key);
				storage.remove(context,key);
				// remoteListeners(key,value,null,false);
								
			    RemoveAckMessage msg = new RemoveAckMessage(key,true);
				msg.setContext(context);
				endPoint.route (null, msg, removeMessage.getNH());

			}
			catch(StorageException ex)
			{
				ex.printStackTrace();
				// send reject ack ?
			}
		
	      break;
          case MessageType.REMOVE_ACK_MESSAGE:
			  
			  RemoveAckMessage removeAckMessage = (RemoveAckMessage) message;
			  key = removeAckMessage.getKey();			  
			  successful = removeAckMessage.getSuccessful();
			 		  	
			  if (remove_clients!=null) {
			    
			    // Client is only for one time
				Vector<BunshinRemoveClient> id_clients = remove_clients.remove(key);
				if (id_clients!=null) {
				
				  for(int i=0; i<id_clients.size(); i++)	{
				    BunshinRemoveClient client = id_clients.get(i);
				    client.remove(successful);
				  }
				}
			  }
			  
		  break;
  	      case MessageType.REMOVE_REPLICA_MESSAGE:

			RemoveReplicaMessage remReplicaMsg = (RemoveReplicaMessage) message;
			
			NodeHandle owner = (NodeHandle) replicaKeyOwner.get(remReplicaMsg.getKey());
			if (owner!=null && owner.equals(remReplicaMsg.getNH())) {
				replicaKeyOwner.remove(remReplicaMsg.getKey());
			}

			if (owner!=null && replicaBuckets.containsKey(owner)) {
				
				StorageManager replicaStorage = (StorageManager) replicaBuckets.get(owner);
				Bucket replicas = (Bucket) replicaStorage.getBucket(context);

				log("deliver","Replicas "+replicas);
				if (replicas!=null) {
					
					try {

					  replicas.remove(remReplicaMsg.getKey());					
					  if (replicas.size()>0)  replicaStorage.put(context,replicas);
					  else  replicaStorage.removeBucket(context);

					  replicaBuckets.put(remReplicaMsg.getNH(),replicaStorage);
					  log("deliver","Replicas Final "+replicaBuckets);

					}
					catch(StorageException ex)
					{
						ex.printStackTrace();
					}
				}
			}

		   break;
  	       case MessageType.GET_MESSAGE:
  	    	
  	    	GetMessage getMsg = (GetMessage) message;
			int phase = getMsg.getPhase();
			
			switch(phase) 	{
				case 1 :
				
				    key = getMsg.getKey();
					value = storage.extract(context,key);
                    					
					GetMessage msg = new GetMessage(key,value);
					msg.setContext(context);
					
					//System.out.println("GET REQUEST <"+key+"> from "+mesg.getNH());
					
					endPoint.route (null, msg, getMsg.getNH());

					break;
					
				case 2 :
					
					key = getMsg.getKey();
					value = getMsg.getValue();
					
					//System.out.println("GET ACK <"+key+"> => "+mesg.getValue());

					if (get_clients!=null) {

						// Client is only for one time
						Vector<BunshinGetClient> id_clients = get_clients.remove(key);
                        if (id_clients!=null) {
                        
						  for(int i=0; i<id_clients.size(); i++) {
						  
							BunshinGetClient client = id_clients.get(i);
							client.get(value);
						  }
						}  
					}

					break;
			}

		
	
 	        break;
            case MessageType.REPLICA_MESSAGE:
	
			
			  ReplicaMessage replicaMsg = (ReplicaMessage) message;
			  key = replicaMsg.getKey();
			  Serializable values = replicaMsg.getValues();


  			  if (!storage.exists(context,key) && values!=null) {				

				putReplica(replicaMsg.getNH(),context,key,values);

				ReplicaAckMessage msg = new ReplicaAckMessage(key,endPoint.getLocalNodeHandle());
				msg.setContext(context);
				endPoint.route (null, msg, replicaMsg.getNH());

				// #DEBUG
				//System.out.println("REPLICA REQUEST :"+key);
				log("deliver","Saving replica in context"+context+" values : "+values+" of "+replicaMsg.getNH().getId());
			}
	      
		    break;
	        case MessageType.REPLICA_ACK_MESSAGE:
	    				
			  ReplicaAckMessage replicaAckMsg = (ReplicaAckMessage) message;
			  key = replicaAckMsg.getKey();
			  Collection<NodeHandle> c = replicaNHs.get(context,key);
			  if (c==null) 	{
				c = new Vector<NodeHandle>();
			  }
			  c.add(replicaAckMsg.getNH());
			  replicaNHs.put(context,key,c);
			  log("deliver","Replica ack "+key+" of "+replicaAckMsg.getNH().getId());

		    break;
 	        case MessageType.LEAVE_MESSAGE:
 	    	
			  LeaveMessage leaveMsg = (LeaveMessage) message;
			  NodeHandle source = leaveMsg.getNH();

			  // extract content : Hashtable(context(Hashtable(key,value))
			  if (leaveMsg.isContent()) {
				Hashtable<String,Bucket> newBuckets = (Hashtable<String,Bucket>) leaveMsg.getContent();
				for (String context2 : newBuckets.keySet()) {
					Bucket newKeys = (Bucket) newBuckets.get(context2);

					if (newKeys.size()>0) {
						try {
							storage.add(context,newKeys);
							log("deliver","New keys received : "+newKeys);
						}
						catch(StorageException ex) 	{
							ex.printStackTrace();
							// activate threshold?
							// send reject ack ?
						}
					}
				}
			}

			// System.out.println(replicaBuckets);
			// System.out.println("Deleting "+mesg.getNH().getId()+" entries");

			// remove replicas of a given owner

			StorageManager replicas = (StorageManager) replicaBuckets.remove(source);
			if (replicas!=null) {
				Hashtable<String,Bucket> buckets = replicas.getBuckets();
				if (buckets!=null) {
					for (Bucket bucket : buckets.values()) {
						for (Id k : bucket.keySet()) {							
							owner = (NodeHandle) replicaKeyOwner.get(k);
							if (owner!=null && owner.equals(source)) replicaKeyOwner.remove(k);
						}
					}
				}
			}

		
		break;
  	    case MessageType.CACHE_MESSAGE:
  	    	
			if (caching) {
				CacheMessage cacheMsg = (CacheMessage) message;
				key = cacheMsg.getKey();
				value = cacheMsg.getValues();

				if (value!=null) cache.put(context,key,value);

				// CACHE_LOG
				log("deliver","Caching key "+key);
			}
		
	    break;
  	    case MessageType.PING_MESSAGE:
	    	
	    	PingMessage pingMsg = (PingMessage) message;
			boolean ack = pingMsg.isAck();
			
			if(!ack) 	{				    				
					PingMessage msg = new PingMessage(endPoint.getLocalNodeHandle(),true);	
					msg.setContext(context);
					endPoint.route (null, msg, pingMsg.getSource());				
			}
			else {
				status.put(pingMsg.getSource(),0);
			}

		
	
	        break;

	  }  	
  	    //case default:		
		//	log ("Deliver Error : Wrong message : "+message);		
  	    //break;
	}


	/** ******************************************************************************* */
	/* BUNSHIN METHODS */
	/** ******************************************************************************* */

    private boolean putLocal(String context, Id key, Serializable value, NodeHandle originator) {
	  try {
		  
	      // #DEBUG
	      //System.out.println("PUT REQUEST ("+key+","+value+")");
		  	
			boolean exist = storage.exists(context, key);
			if (exist) {
				
				log("deliver","Existing value detected for a new insertion of : "+key);
				BunshinMergeClient bmc = mergers.get(context);
				Serializable newValue = value;
				if (bmc!=null) {
					Object oldValue = storage.extract(context, key);
					newValue = (Serializable) bmc.merge(oldValue, value);
				}
								
				storage.write(context,key,newValue);
			    // send replicas
				replicate(context,key,newValue);
				  			  
				return true;
			}
			
			boolean ack = exist;
			
			if (!exist || overwrite) {
		      storage.write(context,key,value);
		      // send replicas
			  replicate(context,key,value);
			  log("deliver","I'm the owner of the "+value);							       	
		  
			//originator is a replica
			  Collection<NodeHandle> c = replicaNHs.get(context,key);
			  if (c==null) {
				  c = new Vector<NodeHandle>();
			  }
			  c.add(originator);
			  replicaNHs.put(context,key,c);
			  
			  ack = true;
			}
			else ack = false;
			
			return ack;
		}
		catch(StorageException ex)	{
			ex.printStackTrace();
			// activate threshold?
			// send reject ack ?
		}
	
		return false;
		
	}

/**
	 * Stores the (key,value) pair in the Bunshin DHT in the default context and
	 * with default field
	 * 
	 * @param key
	 *            Id of the value
	 * @param value
	 *            Serializable to store
	 */
	public void put(Id key, Serializable value) {
		put(Context.DEFAULT_CONTEXT,key,value);
	}
	
	/**
	 * Stores the (context,key,value) in the Bunshin DHT with default field
	 * 
	 * @param context
	 *            Id of the context
	 * @param key
	 *            Id of the value
	 * @param value
	 *            Serializable to store
	 */  
	public void put(String context, Id key, Serializable value) {
     	
		if (isOwner(key)) {
			
			try {             		      
		          		      	
			  	storage.write(context,key,value);
				replicate(context,key,value);
				log("put","I'm the owner of the "+key);
				
				
				// Client is only for one time
			    Vector<BunshinPutClient> id_clients = put_clients.remove(key);
			    if (id_clients!=null) {
			
			      for(int i=0; i<id_clients.size(); i++)	{
			        BunshinPutClient client = id_clients.get(i);
			        client.put(true,endPoint.getLocalNodeHandle()); //fake
			      }
			    }		
			}
			catch(StorageException ex){
				ex.printStackTrace();
				// activate threshold?
				// send local reject ack
			}
		}
		else {
			
			log("put","I'm not the owner of the "+key+", sending ...");
			PutMessage msg = new PutMessage(key,value,endPoint.getLocalNodeHandle());
			msg.setContext(context);
			endPoint.route(key,msg, null);
		}
	}



  /**
	 * Stores the (key,value) pair in the Bunshin DHT in a default field
	 * 
	 * @param key
	 *            Id of the value
	 * @param value
	 *            Object to store
	 * @param client
	 *            listener BunshinPutClient
	 */
	public void put(Id key, Serializable value, BunshinPutClient client) {
		
		Vector<BunshinPutClient> id_clients;

	    if (!put_clients.containsKey(key)) id_clients = new Vector<BunshinPutClient>();
		else id_clients = put_clients.get(key);

		id_clients.add(client);
		put_clients.put(key,id_clients);
		
		put(Context.DEFAULT_CONTEXT,key,value);
	}

	   /**
		 * Stores the (context,key,value) in the Bunshin DHT with result
		 * listener
		 * 
		 * @param context
		 *            String context
		 * @param key
		 *            Id of the value
		 * @param value
		 *            Serializable to store
		 * @param client
		 *            listener BunshinPutClient
		 */  
	public void put(String context, Id key, Serializable value, BunshinPutClient client)
	{
		
		Vector<BunshinPutClient> id_clients;

	    if (!put_clients.containsKey(key)) id_clients = new Vector<BunshinPutClient>();
		else id_clients =  put_clients.get(key);

		id_clients.add(client);
		put_clients.put(key,id_clients);
		
		put(context,key,value);
	}

	  /**
	 * Returns the value of the specific key using the Client application
	 * 
	 * @param key
	 *            Id of the value
	 * @param client
	 *            listener that implements BunshinClient
	 */
	public void get(Id key, BunshinGetClient client) {
		get(Context.DEFAULT_CONTEXT,key,client);
	}

  /**
	 * Returns the values of the specific key and sets the Client application
	 * 
	 * @param context
	 *            Id of the context
	 * @param key
	 *            Id of the value
	 * @param client
	 *            listener that implements BunshinClient
	 */  
	public void get(String context, Id key, BunshinGetClient client) {

        Serializable value = null;        
        boolean found = false;
       
		if (isOwner(key)) {
		    			
			value = storage.extract(context,key);
            if (value!=null) found = true;
			
		}				
		if (!found) {
		  // I'm not owner, but I have a replica?
		  value = getReplicaValue(context,key);
		  if (value!=null) found = true;			
		}
		
		// I'm not owner and don't have any replica, but I have value-cache
		if (!found && cache.contains(context,key)) {
			value = cache.get(context,key);			
			if (value!=null) found = true;
		}		
		
		if (found) {				   
          client.get(value);		  
		}
		
		else{

			Vector<BunshinGetClient> id_clients;

			if (!get_clients.containsKey(key)) id_clients = new Vector<BunshinGetClient>();
			else id_clients =  get_clients.get(key);

			id_clients.add(client);
			get_clients.put(key,id_clients);

			GetMessage msg = new GetMessage(key,endPoint.getLocalNodeHandle());
			msg.setContext(context);
			endPoint.route(key, msg, null);
		}
	}
	
	  /**
		 * Returns the value of the specific key if the values is stored in the
		 * local node
		 * 
		 * @param key
		 *            Id of the value
		 */   
	  public Serializable getLocal(Id key) {
		  return getLocal(Context.DEFAULT_CONTEXT,key);
	  }
	   
	  /**
		 * Returns the value of the specific key if the values is stored in the
		 * local node
		 * 
		 * @param context
		 *            Id of the context
		 * @param key
		 *            Id of the value
		 * 
		 */   
	  public Serializable getLocal(String context, Id key) {
		  	  
		    Serializable value = null;	        
	        //boolean found = false;
	       
			if (isOwner(key)) {
				value = storage.extract(context,key);
			}
			// I'm not owner but I have replica
			else if (replicaKeyOwner.containsKey(key)) 	{
				value = getReplicaValue(context,key);
			}
			// I'm not owner and don't have any replica, but I have value-cache
			else if (cache.contains(context,key)) {
				value = cache.get(context,key);			
			}			
		
			return value;			
		  
	  }
	
	/**
	 * Removes the key/value of the Bunshin DHT
	 * 
	 * @param key
	 *            Id of the value
	 */ 
	public void remove(Id key)	{
		remove(Context.DEFAULT_CONTEXT,key);
	}
   
   /**
	 * Removes the (context,key)/value of the Bunshin DHT
	 * 
	 * @param context
	 *            Id of the context
	 * @param key
	 *            Id of the value
	 */  
	public void remove(String context, Id key) {
				
		// FIX:   billmcc 30 January 2007   
		boolean success = false;
		
		if (isOwner(key)) {
			
			try {    	      	      
			  
			    undoReplica(context,key);
				log("remove","Removing "+key);
				Serializable value = storage.remove(context,key);
				// FIX:   billmcc 30 January 2007   
				success = ( null != value );
								
				//
				// FIX:   billmcc 30 January 2007   
				// notify the remove client that the remove was successful
				// if the key was mastered on the local node.
				// And remove the client from the pending list.
				//
				if( null != remove_clients ) 	{
					Vector<BunshinRemoveClient> id_clients = remove_clients.remove( key );
					if( id_clients != null ) {
						for( int i = 0; i < id_clients.size(); i++ ) {
							BunshinRemoveClient client = id_clients.get(i);
							client.remove( success );
						}
					}
				}			
				// ENDFIX:  billmcc 30 January 2007			   
			}
			catch(StorageException ex)
			{
				ex.printStackTrace();
				// activate threshold?
				// send local reject ack
			}
		}
		else {
			
			RemoveMessage msg =  new RemoveMessage(key,endPoint.getLocalNodeHandle());
			msg.setContext(context);
			endPoint.route(key,msg, null);
		}
	}	
	 
	/**
		 * Removes the key/value of the Bunshin DHT
		 * 
		 * @param key
		 *            Id of the value
		 * @param client
		 *            listener BunshinRemoveClient
		 */  
	  public void remove(Id key, BunshinRemoveClient client) {
		  remove(Context.DEFAULT_CONTEXT,key, client);
	  }

	
	   /**
		 * Removes the (context,key)/value of the Bunshin DHT
		 * 
		 * @param context
		 *            Id of the context
		 * @param key
		 *            Id of the value
		 * @param client
		 *            listener BunshinRemoveClient
		 */  
	  public void remove(String context, Id key, BunshinRemoveClient client) {
		     
		  Vector<BunshinRemoveClient> id_clients;

		    if (!remove_clients.containsKey(key)) id_clients = new Vector<BunshinRemoveClient>();
			else id_clients =  remove_clients.get(key);

			id_clients.add(client);
			remove_clients.put(key,id_clients);
				
			remove(context,key);
	  }

	/**
	 * Inserts in the structure of replicas a new (key,value) from a specific
	 * node
	 * 
	 * @param own
	 * @param key
	 * @param value
	 */
	private void putReplica(NodeHandle own, String context, Id key, Serializable value) {
		
		StorageManager replicas;
		try {
			if (replicaBuckets.containsKey(own)) {
				replicas = (StorageManager) replicaBuckets.get(own);
			}
			else {
				replicas = new MemStorage(); // not persistence for replicas
			}
			
			log("putReplica", "context "+context+" key : "+key+" value "+value+" --> "+replicas.getBucket(context));
			
			if (context!=null && key!=null && value!=null) {
			  replicas.write(context,key,value);

			  replicaBuckets.put(own,replicas);
			  replicaKeyOwner.put(key,own);
			}  

		}
		catch(StorageException ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * Sends a replica of the pair (key,value) to replicationFactor nodes from
	 * replicaSet
	 * 
	 * @param key
	 * @param value
	 */
	private void replicate(String context, Id key, Serializable values) {
		// reset possible replicas
		replicaNHs.remove(context,key);

		NodeHandle nh = endPoint.getLocalNodeHandle();
		Collection<NodeHandle> c = Utilities.convert(endPoint.replicaSet(key,replicationFactor+1));
		if (c!=null) {

			for (NodeHandle to : c) {
				if (!to.getId().equals(nh.getId())) {
					log("replicate","sending replica to "+to.getId());
					// endPoint.route(null,new
					// ReplicaMessage(key,values,nh),to);
					ReplicaMessage msg = new ReplicaMessage(key,values,nh);

					msg.setContext(context);
					endPoint.route(null,msg,to);
				}
			}
		}
	}

	/**
	 * Sends remove replica of the all keys to replica nodes
	 * 
	 * @param key
	 */
	private void undoReplica(String context,Id key) {

		Collection<NodeHandle> c = replicaNHs.remove(context,key);
		if (c!=null) {

			for (NodeHandle nh : c)	{
				
				log("undoReplica","removing replica from "+nh.getId());
				// endPoint.route(null,new RemoveReplicaMessage(nh,key),nh);
				RemoveReplicaMessage msg = new RemoveReplicaMessage(nh,key);
				msg.setContext(context);
				endPoint.route(null,msg,nh);
			}
		}
	}


	/**
	 * Finds in the replicated buckets a specific value in the context bucket
	 * 
	 * @param context
	 * @param key
	 * @return Values values
	 */
	private Serializable getReplicaValue(String context, Id key) {

		boolean found = false;
		Serializable value = null;

		Iterator<StorageManager> it = replicaBuckets.values().iterator();

		while(it.hasNext() && !found) {
			StorageManager s = (StorageManager) it.next();
			value = s.extract(context,key);
			found = (value!=null);
		}
		return value;
	}
	
	private synchronized void refresh(int mode) {
		//if ( mode==0)
		//{
			checkMyKeys();
			checkMyRemoteReplicas();
			checkReplicas();
		//}
		//else if (mode==1)
		//{
		//	checkMyKeys();
		//}
		//else
		//{
		//	checkMyRemoteReplicas();
		//	checkReplicas();
		//}

	}
	
	
	class BunshinAutoPutClient implements BunshinPutClient {
   	  	
   	  	String context;   	  	
   	  	Id key;
   	  	
   	  	public BunshinAutoPutClient(String context, Id key) {
   	  	  this.context = context;
   	  	  this.key = key;
   	  	}
   	  	
  	    public void put(boolean ack, NodeHandle owner) {  	  	
  	      if (ack) {
  	       	try {
  	       		log("bunshinAutoPutClient","key move completed : "+key+" on the : "+owner);
  	       	    Serializable values = storage.extract(context, key);
  	       		storage.remove(context,key);  	       	
  	       	    if (owner!=null && values!=null) putReplica(owner,context,key,values);
  	       	} catch(Exception e) {
  	       		e.printStackTrace();
  	       	}  	         
  	      }
  	    	
	    }
    }
    
    

	class PeriodicTask extends TimerTask
	{

		public void run() 	{
			// keys and replicas
			refresh(0);
			
			//check status
			for (NodeHandle nh : status.keySet()) {
				int state = status.get(nh);
				//if unknown --> waiting
				//if (state==2) {
					PingMessage msg = new PingMessage(endPoint.getLocalNodeHandle(),false);
					msg.setContext("ping");
					endPoint.route(null, msg, nh);
					status.put(nh,3);
				//}
				//if waiting --> dead	
				if (state==3) status.put(nh,1);
			}

			// caches
			if (caching) {
				cache.incTime();

				// caching?
				for (String context : control.keySet()) {
					Hashtable<Id, PriorityList> pairs = control.get(context);
					if (pairs!=null) {
						for (Id key : pairs.keySet()) {
							Serializable values = null;
 
							if (isOwner(key)) {
								values = storage.extract(context,key);
							}
							else if (replicaKeyOwner.containsKey(key)) {
								values = getReplicaValue(context,key);
							}
							else if (cache.contains(context,key)) {
								values = cache.get(context,key);
							}

							PriorityList pl = (PriorityList) pairs.get(key);
							log("cacheThread","cache key "+key+" max request : "+pl.getMax());
							if (pl.getMax()>MAX_REQUEST_TIME) 	{

								NodeHandle nh = (NodeHandle) pl.peak();
								log("cacheThread","send CacheMessage (key : "+key+", values : "+values);
								// endPoint.route (null, new
								// CacheMessage(key,values), nh);
								CacheMessage msg = new CacheMessage(key,values);
								msg.setContext(context);
								endPoint.route (null, msg, nh);
							}
							pl.clear();
						}
					}
				}
			}
		}
	}

}

