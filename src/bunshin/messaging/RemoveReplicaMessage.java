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

package bunshin.messaging;

import java.io.IOException;
import java.net.URL;
import java.util.Hashtable;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 *
 * The replica remove message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class RemoveReplicaMessage extends BunshinMessage {

  private Id key;
  private Object value;
  private NodeHandle source;
	
  public RemoveReplicaMessage(NodeHandle nh, Id key) {  	
    this.source = nh;
  	this.key = key;    	
  }  
  
  public Id getKey() {
  	return key;  	
  }
  
  public NodeHandle getNH() {
  	return source;
  }
  
  public int getPriority() {
  	return Message.MEDIUM_HIGH_PRIORITY;
  }
  
  public String toString() {
    return "ReplicaRemoveMessage key "+key;
  }
  
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.REMOVE_REPLICA_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
	    
	    Hashtable content = new Hashtable();
	    if (key!=null) content.put("key", key);
	    if (value!=null) content.put("value", value);	    
	    if (source!=null) content.put("source", source);
	    	    
	    super.serialize(buf,content);
	   	   
}

//deserialize constructor
public RemoveReplicaMessage(InputBuffer buf) throws IOException {	  
	  
	  	Hashtable content = super.deserialize(buf);
	  		  	
	  	key = (Id) content.get("key");
	  	value = content.get("value");	  	
	  	source 	  = (NodeHandle) content.get("source");
	  	
	  	
}


}
