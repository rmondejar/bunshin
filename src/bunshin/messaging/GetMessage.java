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


public class GetMessage extends BunshinMessage {

  private Id key;  
  private int phase;
  private Serializable value = null;  
  private NodeHandle nh = null;
  private NodeHandle previousNH = null;  
	
  public GetMessage(Id key,  NodeHandle nh) {  	
  	
  	this.key = key;  	
  	this.phase = 1;	    
    this.nh = nh; 
    this.previousNH = nh;    
    
  }
  
  public GetMessage(Id key, Serializable value) {  	
  	
  	this.phase = 2;  	
    this.key = key;
    this.value = value; 
    
  }  
  
  public void setPreviousNH(NodeHandle previous) {
  	previousNH = previous;  	
  }
  
  public Id getKey() {
  	return key;  	
  }
  
  
  public int getPhase() {
  	return phase;
  }
  
  
  public Serializable getValue() {
  	return value;
  }
  
  public NodeHandle getNH() {
  	return nh;  	
  }  
    
  public NodeHandle getPreviousNH() {
  	return previousNH;  	
  }
  
  public int getPriority() {
  	return Message.MEDIUM_HIGH_PRIORITY;
  }
  
  public String toString() {
    return "GetMessage"+phase+" key : "+key+":"+value;
  }
  
  /***************** Raw Serialization ***************************************/

  protected static final short TYPE = MessageType.GET_MESSAGE;
  
  public short getType() {
	    return TYPE;
  }
  
  public void serialize(OutputBuffer buf) throws IOException {
	    
	    Hashtable<String, Serializable> content = new Hashtable<String, Serializable>();
	    if (key!=null) content.put("key", key);	    
	    if (value!=null) content.put("url", value);
	    content.put("phase", new Integer(phase));
	    if (nh!=null) content.put("nh", nh);
	    if (previousNH!=null) content.put("previousNH", previousNH);
	    	    
	    super.serialize(buf,content);

  }

  //deserialize constructor
  public GetMessage(InputBuffer buf) throws IOException {	  
	  
	  	//key = rice.pastry.Id.build(buf);
	  	Hashtable<?,?> content = super.deserialize(buf);
	  		  	
	  	key = (Id) content.get("key");	  	
	  	value = (Serializable) content.get("url");
	  	phase = (Integer) content.get("phase");
	  	nh 	  = (NodeHandle) content.get("nh");
	  	previousNH  = (NodeHandle) content.get("previousNH");
	  	
}

}