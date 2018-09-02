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

package bunshin.test;

import bunshin.*;
import bunshin.util.*;
import rice.p2p.commonapi.*;
import java.io.*;

import org.doomdark.uuid.*;
import java.util.Vector;

/**
 *
 * This main class executes a massive test, where the insertions and restores of the random key/values are verified.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */
 
public class BunshinPlanetLabTest2 {
  
  public static BunshinConnection c;
  public static Vector keys;
  
  public static String getUID() {
    UUIDGenerator gen = UUIDGenerator.getInstance();
    UUID id = gen.generateRandomBasedUUID();
    return id.toString();
  }
  
  public static void main(String args[]) {
    
    boolean allRight = true;
    BufferedReader dis = new BufferedReader(new InputStreamReader (System.in));
    	
    	
    try {    
    
    c = new BunshinConnection("bunshin.properties");
    int num = (Integer.valueOf(args[0])).intValue();
    keys = new Vector(num);
    
    c.init("bunshin.properties");
      
  
    
    //generate random key/value (jug library)
    for(int i=0;i<num;i++) {    	
 
      String random = getUID();	
      Id key = Utilities.generateHash(random);         
     
      keys.add(key);     
     
    }
    
    
    System.out.println("Press any key to start");
    String input = dis.readLine();  

    
    for(int i=0;i<num;i++) {
    	
       
        Id key = (Id)keys.get(i);   
        
        c.storeObject(key,new Integer(i));
        keys.add(key);      
      
        try {      
          System.out.println("Insert key "+key+" value "+i);
          Thread.sleep(100); 
        } catch (Exception ex) {}
      }
    
    
    
    
    
    
    
    System.out.println("Retrieve keys?");
    input = dis.readLine();  
    
    boolean exit = (input.length()>=1) && (input.charAt(0) == 'n');
    
    while(!exit) {
      	
      
    
      int miss = 0;
      int hits = 0;    
        
        
      for(int i=0;i<num;i++) {     
            
        Id key = (Id)keys.get(i);       
      
        Integer value = (Integer) c.retrieveObject(key);  
      
        boolean successful=true;        
        if (value==null) successful = false;
        else if (value.intValue() != i) successful = false;
                
        int retry = 0;   
        while (!successful && retry<10) {
      	  value = (Integer) c.retrieveObject(key);
      	  retry++;
        }
      
      
        Thread.sleep(100);           
        if (value!=null) {
       	  hits++;
      	      System.out.println("Hit in key "+key);
        }	
        else {
      	  miss++;      
      	   System.out.println("Miss in key "+key+" by null value");
        }
      }
    
    
      System.out.println("Total: "+num+" -> Hits:"+hits+" / Miss:"+miss);
      if (miss>0) allRight=false;
      
      System.out.println("Retrieve keys?");
      input = dis.readLine();      
      exit = (input.length()>=1) && (input.charAt(0) == 'n');
      
      
   }   
   } catch(Exception ex) {
   	 ex.printStackTrace();
   }
   
    c.leave();
    if (allRight) System.out.println("Test successfull");
    else System.out.println("Test failed");
    System.exit(0);
 
  }
}
