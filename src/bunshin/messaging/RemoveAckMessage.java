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
 * The remove ack message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class RemoveAckMessage extends BunshinMessage {

  private Id key;
  private boolean successful;
  
  public RemoveAckMessage(Id key, boolean successful) {  	
    this.key = key;    
    this.successful = successful;
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
    return "RemoveAckMessage key "+key;
  }
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.REMOVE_ACK_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  

public void serialize(OutputBuffer buf) throws IOException {
    
    Hashtable content = new Hashtable();
    if (key!=null) content.put("key", key);    
    content.put("successful", new Boolean(successful));
    
    	    
    super.serialize(buf,content);
   	   
}

//deserialize constructor
public RemoveAckMessage(InputBuffer buf) throws IOException {	  
  
  	Hashtable content = super.deserialize(buf);
  		  	
  	key = (Id) content.get("key");  	
  	successful  = (Boolean) content.get("successful");
  	
  	
}




}