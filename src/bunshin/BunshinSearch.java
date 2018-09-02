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

package bunshin;

import bunshin.listeners.*;
import bunshin.util.*;
import rice.p2p.commonapi.*;
import rice.pastry.PastryNodeFactory;


import java.io.*;
import java.util.*;

/**
 *   This class provides an extended methods of the DHT layer, making 
 *  an owner keyword-search engine over Bunshin layer. 
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */
 
public class BunshinSearch extends BunshinConnection {

  /**
   * Creates a node and stablishs the connection with the network
   * @param String of the path of the properties bunshin file   
   */ 
   public BunshinSearch(String path) throws Exception {       
     super(path);    
   }
  
    /**
   * Creates a node and stablishs the connection with the network
   * @param Properties bunshin file   
   */
  public BunshinSearch(Properties prop) throws Exception { 
     super(prop);  
  }
  
    /**
   * Creates a node and stablishs the connection with the network
   * @param String host
   * @param int port
   * 
   */
  public BunshinSearch(String bootHost, int bootPort) { 
    super(bootHost,bootPort);
  }
  	
  private Set getKeywords(String tokens) {
  	
  	StringTokenizer st = new StringTokenizer(tokens);  	
  	Set keyworks = new TreeSet();
  	
  	while(st.hasMoreTokens()) {   	  
  	  String token = st.nextToken();  	  
  	  if (token.length()>2) {
  	  	keyworks.add(token);
  	  }	
  	}  	
  	return keyworks;
  }	  
  
  
  /**
   * Generate the key for one serializable value and inserts in the network.
   * @param String tokens, exemple = "java api docs";
   * @param Serializable value 
   * @return the Id generate from the byte array of the serializable value
   */
  public Id insert(String tokens, Serializable value) throws IOException {
  	
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	ObjectOutputStream os = new ObjectOutputStream(bos);
	os.writeObject(value);
	os.close();		
	
	byte[] bytes = bos.toByteArray();	
	Id idValue = Utilities.generateHash(bytes);
	
    insert(tokens,idValue,value);	 	
    return idValue;
    
  }
  
  /**
   *  Inserts others keywords to improve search work, example :
   *  insert("java api", value1)      --> java.properties = {IdValue1=(java,api)}
   * 								  --> api.properties  = {IdValue1=(api,java)}
   *  insert("java languaje", value2) --> java.properties = {IdValue1=(java,api); IdValue2=(java,language)}
   * 								  --> languages.properties = {IdValue2=(languages, java)}  	
   *
   * @param String tokens, exemple = "java api docs";
   * @param Id of the key 
   * @param Serializable value     *
   */     
  public void insert(String tokens, Id idValue, Serializable value) throws IOException {
  	
    System.err.println( "Insert: " + tokens + " id " + idValue.toStringFull() );
  	Set keywords = getKeywords(tokens);  
  	
  	super.storeObject("#keywords", idValue,(Serializable)keywords);
  	super.storeObject(idValue,value);
  	
  	Iterator it = keywords.iterator();
  	while(it.hasNext()) {   	  
  	 
  	  String token = (String) it.next();
  	  
  	  Set keywords1 = (Set) ((TreeSet) keywords).clone();  	    
  	  
  	  Id key = Utilities.generateHash(token);
      System.err.println( "Insert: lookup " + token + " key " + key.toStringFull() );
  	  Hashtable prop = (Hashtable) super.retrieveObject("#index", key);  	  
  	    
  	  if (prop==null) {
  	  	System.out.println("New prop + "+keywords1);
  	    prop = new Hashtable();  	    
  	    prop.put(idValue,keywords1);
  	  }	
  	  else {  	  	
  	  	Set previous = (Set) prop.get(idValue);  	  	
  	  	System.out.println("Old prop --> "+previous+" + "+keywords1);
  	  	if (previous!=null) keywords1.addAll(previous); 
  	  	prop.put(idValue,keywords1); 	  	
  	  } 	    	  	
  	  super.storeObject("#index",key,(Serializable)prop);  	  
  	}    	  	
  }  	  
  
  /**
   *  Merges all properties, example for (query_key = java api)
   *  java.properties = {IdValue1=(api,docs); IdValue2=(languaje)}
   *  api.properties  = {IdValue1=(java,docs)}
   *  merge(java,api) = {IdValue1=(java,api,docs),IdValue2=(java,language)}  	 
   *
   *  @param String of the query, example = "java api"
   *  @return ResultSortedQueue with the found id's and the number of matches
   */
  public ResultSortedQueue query(String query_key) throws IOException {  	  	
  	
    System.err.println( "Query: " + query_key );
  	 Set keywords = getKeywords(query_key);  	   	  
  	 ResultSortedQueue result = new ResultSortedQueue(keywords);
  	 
  	 Hashtable merge = new Hashtable();
  	  
  	 Iterator it = keywords.iterator();
  	 while(it.hasNext()) {  	   
  	   
  	   String token = (String) it.next();
  	   Id key = Utilities.generateHash(token);

       System.err.println( "Query token " + token  + " key " + key.toStringFull());
       Hashtable prop = (Hashtable) super.retrieveObject("#index", key);
  	          	  	    
  	   if (prop!=null) {
  		 
  	     Iterator it2 = prop.keySet().iterator();  	         	         	     
  	     while(it2.hasNext()) {
  	        Id idValue = (Id) it2.next();
  	        if (merge.containsKey(idValue)) {
  	          Set keywordSet = (Set) merge.get(idValue);
  	          System.out.println("previous "+keywordSet+" + new "+keywords);		          
  	          keywordSet.add(token);
  	          merge.put(idValue,keywordSet);
  	        }
  	        else {
  	          Set keywordSet = new TreeSet();	
  	          keywordSet.add(token);
  	          merge.put(idValue,keywordSet);
  	        } 	       
  	     }
  	   }	
     }
      	 
     //Insert merge result in the HashSortedQueue 	 
  	 Iterator it2 = merge.keySet().iterator();  
  	 while (it2.hasNext()) {
  	   Id idValue = (Id) it2.next();	
  	   Set elems = (Set) merge.get(idValue); 
  	   //System.out.println("!!! id "+idValue+" set :"+elems); 	   
  	   result.add(idValue,elems);  	   
  	 }
  	  
  	 return result; 
  }    
  
  /**
   * Removes the value of the specific key and their keyword indexes
   * @param String tokens, example = "java api docs"
   * @param Id of the key
   */
  public void remove(String tokens, Id idValue) throws IOException {

    System.err.println( "IN REMOVE !!!! " + tokens + " id " + idValue.toStringFull() );
	
	Set keywords = null;
	if (tokens!=null) {
  	  keywords = getKeywords(tokens);
	}
	
	if (keywords==null || keywords.isEmpty()) {
	  keywords = (Set) super.retrieveObject("#keywords",idValue);	
	}
	super.removeObject("#keywords", idValue);
	super.removeObject(idValue);
  	
  	if (keywords!=null) {
  	  Iterator it = keywords.iterator();
  	  while(it.hasNext()) {   	  
  	 
  	   String token = (String) it.next();
  	  
  	   Set keywords1 = (Set) ((TreeSet) keywords).clone();  	    
  	  
  	   Id key = Utilities.generateHash(token);
  	   Hashtable prop = (Hashtable) super.retrieveObject("#index", key);  	  
  	    
  	   if (prop!=null) {  	  	
  	  	  Set previous = (Set) prop.get(idValue);  	  	
  	  	  if (previous!=null) {  	  	
  	  	    prop.remove(idValue);   	  	  	  	  
  	  	    super.storeObject("#index", key,(Serializable)prop);  	    	  
  	  	  }  	  	
  	    }  	  
  	  }
  	}
  }
  
  
    private class BunshinGetClient implements bunshin.listeners.BunshinGetClient
    {
        public boolean lookupResultArrived = false;
        public Serializable value = null;
        public String field;

        public BunshinGetClient( String _field )
        {
            field = _field;
        }

        public void get (Serializable result) {
          synchronized (value) {
            if (result != null) {
                value = result;               
            }
            else {
              value = null;
            }
            lookupResultArrived = true;
          }
        }
    }	
}
