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
 * 
 * @author Ruben Mondejar
 */
public interface StorageManager {
 
  public void init(Properties prop);
  
  public void add(String context, Bucket newBucket) throws StorageException ;

  public void put(String context, Bucket newBucket) throws StorageException ;
  
  public void removeBucket(String context) throws StorageException;
	
  public void write(String context,Id key, Serializable value) throws StorageException;
 
  public Serializable extract(String context,Id key);

  public Serializable remove(String context,Id key) throws StorageException;
      
  public Bucket getBucket(String context);

  public Hashtable<String, Bucket> getBuckets();
  
  public boolean exists(String context,Id key);
  
  public boolean isFull();
  
  public void loadContext(String context);
  
  public Collection<String> getContexts();
  
  public int getContextSize(String context);

  
  
}

