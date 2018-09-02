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
import java.util.*;


/**
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */
 
public class BunshinSearchTest  {	
 
  public static void main(String args[]) {

  try {

  	BufferedReader dis = new BufferedReader(new InputStreamReader (System.in));
  	
    BunshinSearch c = new BunshinSearch("bunshin.properties");
    
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

      	System.out.println ("Enter the Keyworks...");
    	String keywords = dis.readLine();    	
    	System.out.println ("Enter the String to store...");
    	String value = dis.readLine();
    	Id idValue = Utilities.generateHash(value);
        c.insert(keywords,idValue,value);
        System.out.println("Operation successful, id generated --> "+idValue);
         
        System.out.println("Press <RETURN> to exit...");
        System.in.read();
        c.leave();
      }
      catch (IOException ex) {
      	ex.printStackTrace();
      }
    }
    else if (whatToDo==2){
        try {
        
          c.init("bunshin.properties");

      	  System.out.println ("Enter Keywords to search...");
          String keywords = dis.readLine();
          ResultSortedQueue rsq = c.query(keywords);
          if (rsq!=null) {
          	 System.out.println(rsq);
          	 Iterator it = rsq.iterator();
          	 System.out.print("Objects : ");
          	 while (it.hasNext()){
          	   Id key = (Id) it.next();
          	   Object obj = c.retrieveObject(key);	
          	   System.out.print(obj+", ");
          	 }
          	 System.out.println();
          }
          else System.out.println("Your search - "+keywords+" - did not match any objects");
  
          System.out.println("Press <RETURN> to exit...");
          System.in.read();
          c.leave();
        }
        catch (IOException ex) {
        	ex.printStackTrace();
        }
      }

    System.exit(0);
    }        catch (Exception ex) {
        	ex.printStackTrace();
        }
  }
}
