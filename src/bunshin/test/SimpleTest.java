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

import junit.framework.*;
import bunshin.storage.*;
import bunshin.*;

import java.util.*;
import rice.p2p.commonapi.*;
import bunshin.util.*;


/**
 * Bunshin's main testcase
 *
 * @author Ruben Mondejar  <ruben.mondejar@estudiants.urv.es>
 */
public class SimpleTest extends TestCase {

  protected static BunshinSearch[] bunshin;
  protected final static int num = 5;
  protected static Id global_id = null;

  public SimpleTest(String name) {
	super(name);
  }

  protected void setUp() {
		
    try {
  	  bunshin = new BunshinSearch[num];
  	  for (int i=0; i<num; i++) {  	  	
  	  	bunshin[i] = new BunshinSearch("auto",5009);
  	  	Thread.sleep(1000);
  	    bunshin[i].init("BunshinTest", new MemStorage(),3,8,false,true);  	    
  	  }		  
	  
	} catch (Exception e1) {
	  e1.printStackTrace();	
	}	
  }
  
  public static void finishTest() {
	  for (int i=0; i<num; i++) {
	  	bunshin[i].leave(); 	      	    
  	  }		  
  	
  }


  public static Test suite() {
    return new TestSuite(SimpleTest.class);
  }


  public void testStore() throws Exception {    
	
    String id = "time";
    long time = System.currentTimeMillis();
                  
    Id key = bunshin[0].storeObject(id,new Long(time));    
      
    Thread.currentThread().sleep(5000);
    
    long time2 = System.currentTimeMillis();
                  
    bunshin[0].storeObject(id,new Long(time2));    
      
    Thread.currentThread().sleep(5000);
    
    Long tr = (Long) bunshin[num-1].retrieveObject(key);    
     		
	assertEquals(time2,tr.longValue());
    
	finishTest();
  }
 

  
  public void testStoreDuplicate() throws Exception {    
	
    String id = "time";
    long time = System.currentTimeMillis();
                  
    Id key = bunshin[0].storeObject(id,new Long(time));    
      
    Thread.currentThread().sleep(5000);
    
    long time2 = System.currentTimeMillis();
                  
    bunshin[1].storeObject(id,new Long(time2));    
      
    Thread.currentThread().sleep(5000);
    
    Long tr = (Long) bunshin[num-1].retrieveObject(key);    
     		
	assertEquals(time2,tr.longValue());
	
	finishTest();
  }
  
   public void testStoreTriple() throws Exception {    
	
    String id = "time";
    long time = System.currentTimeMillis();   
               
    Long v1 = new Long(time);              
    
    Id key = bunshin[0].storeObject(id,v1);    
      
    Thread.currentThread().sleep(5000);
    
    long time2 = System.currentTimeMillis();                     
    Long v2 = new Long(time2);        
                  
    bunshin[1].storeObject(id,v2);    
      
    Thread.currentThread().sleep(5001);
    
    long time3 = System.currentTimeMillis();                    
    Long v3 = new Long(time3);    
                  
    bunshin[2].storeObject(id,v3);    
      
    Thread.currentThread().sleep(5001);
    
    Long tr = (Long) bunshin[num-1].retrieveObject(key);    
     		
	assertEquals(time3,tr.longValue());
	
	finishTest();
  }
  
	  
   public void testMultiContext() throws Exception {    
	
		String key = "time";
	    Id id = Utilities.generateHash(key);

		long time = System.currentTimeMillis();
                  
		bunshin[0].storeObject("context1",id,new Long(time));    

      
		Thread.currentThread().sleep(5000);
    
		long time2 = System.currentTimeMillis();
                  
		bunshin[1].storeObject("context2",id,new Long(time2));    
      
		Thread.currentThread().sleep(5000);
    
		Long tr = (Long) bunshin[num-1].retrieveObject("context1",id);    
     		
		assertEquals(time,tr.longValue());

	    tr = (Long) bunshin[num-1].retrieveObject("context2",id);    
     		
	    assertEquals(time2,tr.longValue());
	
		finishTest();
	}
  
	
  
    	
  	
}