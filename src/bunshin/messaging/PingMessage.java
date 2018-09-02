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
import java.io.Serializable;
import java.net.URL;
import java.util.Hashtable;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import bunshin.util.*;

/**
 *
 * The get message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */


public class PingMessage extends BunshinMessage {
  
  private NodeHandle source; 
  private boolean ack;
  
  public PingMessage(NodeHandle source, boolean ack) {
	  this.source = source;
	  this.ack = ack;
  }
  
  public boolean isAck() {
	  return ack;
  }
  
  public NodeHandle getSource() {
	return source;  
  }
  
  public int getPriority() {
  	return Message.HIGH_PRIORITY;
  }
  
  public String toString() {
    return "PingMessage from "+source+" ack? : "+ack;
  }
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.PING_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
	    
	    Hashtable<String, Serializable> content = new Hashtable<String, Serializable>();
	    if (source!=null) content.put("source", source);	    
	    content.put("ack", new Boolean(ack));
	    	    
	    super.serialize(buf,content);

  }

  //deserialize constructor
  public PingMessage(InputBuffer buf) throws IOException {	  
	  
	  	//key = rice.pastry.Id.build(buf);
	  	Hashtable<?,?> content = super.deserialize(buf);
	  		  	
	  	source = (NodeHandle) content.get("source");
	  	ack = (Boolean) content.get("ack");	  	
}

}