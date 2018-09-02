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
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 *
 * Util methods used in the bunshin classes
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */
 
public class Utilities {
	
	  public static Properties loadProp(String path) throws Exception {
		  	FileInputStream  fis = new FileInputStream(path);
			Properties prop = new Properties();
			prop.load(fis);
			fis.close();
		  	return prop;
	}
	
	public static Collection<NodeHandle> convert(NodeHandleSet set) {
	   int size = set.size();	
	   Vector<NodeHandle> v = new Vector<NodeHandle>(size);	
	   
	   for (int i=0;i<size;i++) {
	   	  NodeHandle nh =  set.getHandle(i);
	   	  v.add(nh);	   	
	   }	   
	   return v;
	}	

    public static Id generateHash (String data) {    	
      return generateHash(data.getBytes());      
    }
    
    public static Id generateHash (byte[] data) {
      MessageDigest md = null;

      try {
        md = MessageDigest.getInstance("SHA");
      }
      catch (NoSuchAlgorithmException e) {
        System.err.println("No SHA support!");
      }

      md.update(data);
      byte[] digest = md.digest();

      Id newId = rice.pastry.Id.build(digest);

      return newId;
    }
    
    /**
     * Generates a hash for the keys contained in the hashtable
     * @param data Hasthable containing the keys
     * @return The NodeId hash object
     */
    public static Id generateHash (Hashtable data) {
      MessageDigest md = null;

      try {
        md = MessageDigest.getInstance ("SHA");
      }
      catch (NoSuchAlgorithmException e) {
        System.out.println ("No SHA support!");
      }

      Iterator i = data.keySet().iterator();

      String keys = "";

      while (i.hasNext()) {
        keys += (String) i.next();
      }

      //System.out.println ("[generateHash - KeyString] = " + keys);
      md.update(keys.getBytes());
      byte[] digest = md.digest();

      Id newId = rice.pastry.Id.build(digest);

      return newId;
    }
    
    /**
     * Helper method to generate an id string
     *
     * @param data hashtable containing object data
     *
     * @return string containing object id
     */
    /*
    public static String generateKeyId (Hashtable data) {
      String keys = "";
      if (data.containsKey (Context.OBJECTID)) {
        keys = Context.OBJECTID + "_" + (String) data.get (Context.OBJECTID);
      }
      else if (data.containsKey (Context.OBJ)) {
        keys = Context.OBJ + "_" + (String) data.get (Context.OBJ);
      }
      else {
        Iterator i = data.keySet().iterator();

        while (i.hasNext()) {
          keys += (String) i.next() + "_";
        }
      }

      return keys;
    }
  */  
  
}	