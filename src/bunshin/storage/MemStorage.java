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

package bunshin.storage;

import java.io.Serializable;
import java.util.*;

import bunshin.util.*;
import rice.p2p.commonapi.*;

/**
 * @author Ruben Mondejar
 */
public class MemStorage implements StorageManager {

  private Hashtable<String, Bucket> memTable = new Hashtable<String, Bucket>();
  
  public void init(Properties prop) {}
    
  public void add(String context, Bucket newBucket) throws StorageException{
	  if (memTable.containsKey(context)) {
		  Bucket mem = memTable.get(context);
		  mem.add(newBucket);	  
	  }
  }

	public void put(String context, Bucket newBucket) throws StorageException {		
		memTable.put(context,newBucket);	
	}

	public void removeBucket(String context) throws StorageException {		
		memTable.remove(context);	
	}
	
  public void write(String context, Id key, Serializable value) throws StorageException {
	  Bucket mem;
	  if (memTable.containsKey(context))  {
		  mem = memTable.get(context);			  
	  }
	  else  {
		  mem = new Bucket();
	  }
	  
	  mem.overwrite(key,value);	  
	  memTable.put(context,mem);
  }
  
  public Serializable extract(String context, Id key) { 	
	  if (memTable.containsKey(context)) {
		  Bucket mem = memTable.get(context);	
		  return mem.extract(key);	
	  }
	  return null;
  }  
  
  public Bucket getBucket(String context) {
	 if (memTable.containsKey(context)) {
	    Bucket mem = memTable.get(context);	
	    return mem;
	 }
	  return null;
  }   

  public Hashtable<String, Bucket> getBuckets() {
	return memTable;
  }   
  
  public Serializable remove(String context, Id key) {
	  if (memTable.containsKey(context)) {
		  Bucket mem = memTable.get(context);	             
		  return mem.remove(key);	    	  
	  }
	  return null;
  } 
   
  public boolean exists(String context, Id key) {
	  if (memTable.containsKey(context)) {
		  Bucket mem = memTable.get(context);
		  return mem.containsKey(key);	    	  
	  }
	  return false;
  }  
 
  public boolean isFull() {
  	return false;
  }

@Override
public Collection<String> getContexts() {	
	return memTable.keySet();
}

@Override
public int getContextSize(String context) {
	// 
	Bucket b = memTable.get(context);
	return b.size();
}

@Override
public void loadContext(String context) {
	// TODO Auto-generated method stub
	
}
  


}

