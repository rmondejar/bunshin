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
import java.util.Hashtable;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;

/**
 *
 * The leave message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class LeaveMessage extends BunshinMessage {

  private NodeHandle source = null;
  protected Object content = null;
	
  public LeaveMessage(NodeHandle source) {  	
  	this.source = source;  
  }
  
  public LeaveMessage(NodeHandle source, Object content) {  	
  	this.source = source;  
  	this.content=content;
  	
  }  
  
  public NodeHandle getNH() {
  	return source;
  }
      
  public boolean isContent(){
  	return content!=null;
  }
  
  public Object getContent() {
  	return content;
  }
  
  public int getPriority() {
  	return Message.MEDIUM_HIGH_PRIORITY;
  }
  
  public String toString() {
    return "LeaveMessage from "+source+" content "+content;
  }
  
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.LEAVE_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
	    
	    Hashtable pack = new Hashtable();
	    
	    if (content!=null) pack.put("content", content);   
	    if (source!=null) pack.put("source", source);   
	    	    
	    	    
	    super.serialize(buf,pack);
	   	   
	}

//	deserialize constructor
	public LeaveMessage(InputBuffer buf) throws IOException {	  
	  
	  	Hashtable pack = super.deserialize(buf);
	  		  	
	  	content = pack.get("content");
	  	source = (NodeHandle) pack.get("source");  	
	  	
	  	
	  	
	}


}