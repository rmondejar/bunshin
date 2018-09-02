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

/**
 *
 * This main class executes a test with a three different roles. The first role is a 
 * simple Pastry node and Bunshin application registered. In the second role the application 
 * also stores a String in the DHT. The last role restore the String with the given key.
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@urv.cat>
 */
 
public class BunshinTest {
  
  public static void main(String args[]) {
   try {
   
  	BufferedReader dis = new BufferedReader(new InputStreamReader (System.in));
  	
    BunshinConnection c = new BunshinConnection("bunshin.properties");    
    boolean end = false;
    int whatToDo = (Integer.valueOf(args[0])).intValue();
    System.out.println("whatToDo == "+whatToDo);
    if (whatToDo==0) {
      try {
       
        c.init("bunshin.properties");
        
        System.out.println("Press <RETURN> to exit...");
        System.in.read();
        c.leave();
      }
      catch (IOException ex) {
      }
    }
    else if (whatToDo==1){
      try {
        
        c.init("bunshin.properties");
        while (true) {
      	  System.out.println ("Enter the String to store...");
    	  String input = dis.readLine();  
    	  Id id = Utilities.generateHash(input);  	
          c.storeObject(Context.DEFAULT_CONTEXT,id,input);
         }
        //System.out.println("Press <RETURN> to exit...");
        //System.in.read();
        //c.leave();
      }
      catch (IOException ex) {
      	ex.printStackTrace();
      }
    }
    else if (whatToDo==2){
        try {
               
          c.init("bunshin.properties");

        while (true) {
      	  System.out.println ("Enter the String to retrieve...");
          String input = dis.readLine();
          Id id = Utilities.generateHash(input);  
          c.retrieveObject(Context.DEFAULT_CONTEXT,id, new BunshinApplication(input));
        }
          
        //System.out.println("Press <RETURN> to exit...");
         // System.in.read();
          //c.leave();
        }
        catch (IOException ex) {
        	ex.printStackTrace();
        }
      }
    else if (whatToDo==3){
        try {
        
          c.init("bunshin.properties");

      	  System.out.println ("Enter the String to remove...");
          String input = dis.readLine();
          c.removeObject(input);
  
          System.out.println("Press <RETURN> to exit...");
          System.in.read();
          c.leave();
        }
        catch (IOException ex) {
        	ex.printStackTrace();
        }
      }
    System.exit(0);
    }  catch (Exception ex) {
        	ex.printStackTrace();
    }
  }
}
