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
import java.util.*;
 
/**
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class Cache {

  private int TTL = 0;
  private int MAX_ELEM = 0;
    
  private ArrayList<Serializable> elems = new ArrayList<Serializable>(); //value queue
  private ArrayList<Id> keys = new ArrayList<Id>(); //key queue
  private ArrayList<String> contexts = new ArrayList<String>(); // context queue
  private ArrayList<Integer> ttls = new ArrayList<Integer>();
  
  public Cache(int TTL, int MAX_ELEM) {
  	this.TTL = TTL;
  	this.MAX_ELEM = MAX_ELEM;
  	
  }
  
  public synchronized void put(String context, Id key, Serializable values) {
    
    elems.add(values); 
    keys.add(key);  
	contexts.add(context);  
    ttls.add(new Integer(0));
    
    if (keys.size()>MAX_ELEM) {
	  elems.remove(0);
      keys.remove(0);
	  contexts.remove(0);
      ttls.remove(0);      
    }	
  } 
  
  public Serializable get(String context, Id key) {
	if (contains(context,key)) {
      int max = elems.size();
	  for (int i=0;i<max;i++)  {
        if (contexts.get(i).equals(context) && keys.get(i).equals(key))
	      return elems.get(i);
	  } 
	}
    return null;
  }
  
  public boolean contains(String context, Id key) {
  	return (contexts.contains(context) && keys.contains(key));
  }
  
  public synchronized void incTime() {

    Iterator<Integer> it = ttls.iterator();
    int i = 0;
    while (it.hasNext()) {
      int ttl = ((Integer) it.next()).intValue() + 1;	
      if (ttl>TTL) {
        elems.remove(i);  	  	 
  	    keys.remove(i);
        contexts.remove(i);
        ttls.remove(i);        
  	  }
  	  else {  	  	
  	  	i++;
  	  }
      
    }  	
  
  }
  
  
  	
}