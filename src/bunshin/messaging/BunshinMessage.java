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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;

import rice.p2p.commonapi.*;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;
import rice.p2p.scribe.ScribeContent;
import rice.p2p.util.rawserialization.JavaDeserializer;
import rice.p2p.util.rawserialization.JavaSerializationException;
import bunshin.util.*;

/**
 *
 * The bunshin message is the main message and the father of the other messages.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public abstract class BunshinMessage implements RawMessage {
  
  protected static final short TYPE = MessageType.BUNSHIN_MESSAGE;

  protected String context;
  
  public void setContext(String context) {
	  this.context=context;
  }
   
  public String getContext() {
    return context;
  }

  public void serialize(OutputBuffer buf, Hashtable<String, Serializable> content) throws IOException {
	  if (content!=null) {
		  
		content.put("context", context);
		  
	    try {
	      ByteArrayOutputStream baos = new ByteArrayOutputStream();
	      ObjectOutputStream oos = new ObjectOutputStream(baos);
	      
	      
	      oos.writeObject(content);
	      oos.close();
	      
	      byte[] temp = baos.toByteArray();
	      buf.writeInt(temp.length);
	      buf.write(temp, 0, temp.length);
	   } catch (IOException ioe) {
	      throw new JavaSerializationException(content, ioe);
	    }
	  }
  }
 
  public Hashtable<String, Serializable> deserialize(InputBuffer buf) throws IOException {    
	byte[] array = new byte[buf.readInt()];
	buf.read(array);
		
	ByteArrayInputStream baos = new ByteArrayInputStream(array);
	ObjectInputStream ois = new ObjectInputStream(baos);

	try {
	   Hashtable<String, Serializable> content = (Hashtable<String, Serializable>) ois.readObject();
	   context = (String) content.get("context");
	   return content;
	} catch (ClassNotFoundException e) {
	   throw new RuntimeException("Unknown class type in message - cant deserialize.", e);
	}    
  }


  
  
     
  
}