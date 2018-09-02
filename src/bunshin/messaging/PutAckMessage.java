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
 * The put ack message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class PutAckMessage extends BunshinMessage {

  private Id key;  
  private NodeHandle nh;
  private boolean successful;
  
  public PutAckMessage(Id key, boolean successful, NodeHandle source) {  	
    this.key = key;
    this.successful = successful;
    this.nh = source;
  }  
 
  public Id getKey() {
  	return key;  	
  }
  
  public boolean getSuccessful() {
  	return successful;
  }
  
  public int getPriority() {
  	return Message.MEDIUM_HIGH_PRIORITY;
  }
  
  public String toString() {
    return "PutAckMessage key "+key;
  }

  public NodeHandle getSource() {
	return nh;
  }
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.PUT_ACK_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  
  public void serialize(OutputBuffer buf) throws IOException {
	    
	    Hashtable content = new Hashtable();
	    if (key!=null) content.put("key", key);	       
	    if (nh!=null) content.put("nh", nh);   
	    content.put("successful", new Boolean(successful));
	    
	    	    
	    super.serialize(buf,content);
	   	   
	}

//	deserialize constructor
	public PutAckMessage(InputBuffer buf) throws IOException {	  
	  
	  	Hashtable content = super.deserialize(buf);
	  		  	
	  	key = (Id) content.get("key");	  	
	  	nh = (NodeHandle) content.get("nh");  	
	  	successful  = (Boolean) content.get("successful");
	  	
	  	
	}





}