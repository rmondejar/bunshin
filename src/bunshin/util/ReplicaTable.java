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

import java.util.*;
 
/**
 *
 * @author Ruben Mondejar  <ruben.mondejar@urv.cat>
 */

public class ReplicaTable extends Hashtable<String,Hashtable<Id,Collection<NodeHandle>>> {

	private static final long serialVersionUID = 9024131272026979409L;

	public void put(String context, Id key, Collection<NodeHandle> c) {
		
		Hashtable<Id, Collection<NodeHandle>> replicas = this.get(context);
		if (replicas==null) {
			replicas = new Hashtable<Id, Collection<NodeHandle>>();
		}		
		replicas.put(key,c);
		put(context,replicas);

	}

	public Collection<NodeHandle> get(String context, Id key) {
		
		Hashtable<Id, Collection<NodeHandle>> replicas = this.get(context);
		if (replicas!=null) {
			return replicas.get(key);
		}		
		return null;
	}

	public Collection<NodeHandle> remove(String context, Id key) {
		Collection<NodeHandle> c = null;
		Hashtable<Id, Collection<NodeHandle>> replicas = this.get(context);
		
		if (replicas!=null) {
			c = replicas.remove(key);
			if (replicas.size()>0) put(context,replicas);
			else remove(context);
		}
		return c;
	}

	public Collection<NodeHandle> getValues() {
		
	   Set<NodeHandle> c = new HashSet<NodeHandle>();
	   for (String context : this.keySet()) {
         Hashtable<Id, Collection<NodeHandle>> pairs = this.get(context);
         for (Collection<NodeHandle> c2 : pairs.values()) {
           c.addAll(c2);
         }  
	   }
	   return c;
	}



}