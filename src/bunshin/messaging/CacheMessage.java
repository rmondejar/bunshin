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
 * The cache message.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class CacheMessage extends BunshinMessage {
  	
  private Id key;
  private Serializable values = null;  
  
  public CacheMessage(Id key, Serializable values) {  	
    this.key = key;
    this.values = values;     
  }  
 
  public Id getKey() {
  	return key;  	
  }
  
  public Serializable getValues() {
  	return values;
  }
    
  public int getPriority() {
  	return Message.MEDIUM_PRIORITY;
  }
  
  public String toString() {
    return "CacheMessage key "+key+" : "+values;
  }
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.CACHE_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
	    //key	    
	    //buf.writeShort(key.getType());
	    //key.serialize(buf);
	    
	    //buf.writeBoolean(values!=null);	    
        //super.serialize(buf,values);
        
        
        Hashtable content = new Hashtable();
	    if (key!=null) content.put("key", key);
	    if (values!=null) content.put("values", values);
	    	    
	    super.serialize(buf,content);

  }

  //deserialize constructor
  public CacheMessage(InputBuffer buf) throws IOException {	  
	  
	  	key = rice.pastry.Id.build(buf);
	  	
	  	if (buf.readBoolean()) values = super.deserialize(buf);
	 
  }

	  

}