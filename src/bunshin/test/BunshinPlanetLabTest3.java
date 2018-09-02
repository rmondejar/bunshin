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
 
public class BunshinPlanetLabTest3 {
  
  public static String getUID() {
    UUIDGenerator gen = UUIDGenerator.getInstance();
    UUID id = gen.generateRandomBasedUUID();
    return id.toString();
  }
  
    public static void main(String args[]) {

    try {
    
  	BufferedReader dis = new BufferedReader(new InputStreamReader (System.in));

    boolean end = false;
    int whatToDo = (Integer.valueOf(args[0])).intValue();
    System.out.println("whatToDo == "+whatToDo);
    if (whatToDo==0) {
      try {
      	
      	Vector keys = new Vector();
        //generate random key/value (jug library)
        System.out.println("Creating keys");
        for(int i=0;i<400;i++) {    	
 
          String random = getUID();	
          Id key = Utilities.generateHash(random);              
          keys.add(key);          
          System.out.println(i+") "+key);
        }
        
        File f = new File("test.keys");
		f.createNewFile();
		FileOutputStream out = new FileOutputStream(f);
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeObject(keys);
		oos.close();			
      	System.out.println("Finish");
      	
      }
      catch (Exception ex) {
      	ex.printStackTrace();
      }
    }
    else if (whatToDo==1 || whatToDo==2) {
         
      int max = whatToDo*200;
      int min = max - 200;
      
      try {
      	
        BunshinConnection c = new BunshinConnection("bunshin.properties");    
        c.init("bunshin.properties");
        
        File f = new File("test.keys");
        FileInputStream in = new FileInputStream(f);
        ObjectInputStream ois = new ObjectInputStream(in);						  
        //read info									     
		Vector keys = (Vector) ois.readObject();
		ois.close();       
        
          	 System.out.println("Press <RETURN> to start...");
      System.in.read();
   
        System.out.println("Inserting keys ("+min+","+max+")");
        
        for(int i=min;i<max;i++) {
    	       
          Id key = (Id)keys.get(i);   
        
          if (whatToDo==1) c.storeObject(key,new Integer(i));
          else c.storeObject(key,Integer.toString(i));
          
          keys.add(key);      
      
          try {      
            System.out.println("Insert key "+key+" value "+i);
            Thread.sleep(100); 
          } catch (Exception ex) {}
        }      
       
        System.out.println("Press <RETURN> to exit...");
        System.in.read();
        c.leave();
      
      }
      catch (Exception ex) {
      	ex.printStackTrace();
      }
    }
    else { // whatToDo in {3,4,5,6}
         
      int max = (whatToDo-2)*100;
      int min = max - 100;
      
      try {
      	
        BunshinConnection c = new BunshinConnection("bunshin.properties");    
        c.init("bunshin.properties");
        
        File f = new File("test.keys");
        FileInputStream in = new FileInputStream(f);
        ObjectInputStream ois = new ObjectInputStream(in);						  
        //read info									     
		Vector keys = (Vector) ois.readObject();
		ois.close();       
        
        int miss = 0;
        int hits = 0;
        int retry = 0;     
        
          	 System.out.println("Press <RETURN> to start...");
      System.in.read();
   
        
        System.out.println("Retrieving keys ("+min+","+max+")");      
        
        for(int i=min;i<max;i++) {     
            
          Id key = (Id)keys.get(i);       
      
          int value = -1;
      
          boolean successful=true;  
          do {
                 
            try {
           
              if (max<=200) {
          	    Integer v = (Integer) c.retrieveObject(key);  
          	    value = v.intValue();
              }	
              else {
          	    String v = (String) c.retrieveObject(key);  
          	    value = Integer.parseInt(v);
              }
            } catch (Exception e)  {
              successful=false;  
              System.out.println("Exception in key "+key+" -> "+e.getMessage());  
            }
            
            if (value != i) successful = false;
            retry++;
            
          } while (!successful && retry<10);
        
      
      
        Thread.sleep(100);           
        if (successful) {
       	  hits++;
      	      System.out.println("Hit in key "+key);
        }	
        else {
      	  miss++;      
      	   System.out.println("Miss in key "+key);
        }   
    }
      System.out.println("Total: Hits:"+hits+" / Miss:"+miss);
           
      System.out.println("Press <RETURN> to exit...");
      System.in.read();
      c.leave();

      }
      catch (Exception ex) {
      	ex.printStackTrace();
      }
    }
    }
      catch (Exception ex) {
      	ex.printStackTrace();
      }
  } 
}
