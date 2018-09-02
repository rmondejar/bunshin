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

package bunshin.util;

import rice.p2p.commonapi.*;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

/**
 *
 * The bucket, a chunk of hashtable, to belongs to specific key's rank
 *
 * @author Ruben Mondejar  <ruben.mondejar@urv.cat>
 */

public class Bucket extends Hashtable<Id,Serializable> implements java.io.Serializable, Cloneable {
	
	private static final long serialVersionUID = 2086678998423251881L;

	public void add(Bucket newBucket) {
	  if (newBucket.size()>0) super.putAll(newBucket);	
	}

	public synchronized void overwrite(Id key, Serializable value) {      
		if (key!=null && value!=null) super.put(key,value);	
    }
          	
	public synchronized Serializable remove(Id key) {   
       if (key!=null)  return super.remove(key);
       return null;
    }

   	public synchronized Serializable extract(Id key) {   	 	
   		if (key!=null) return super.get(key);
   		return null;
   	}   	
  
   	public String toString() {
   	  return "Bucket : "+super.toString();	
   	}
        
      
}