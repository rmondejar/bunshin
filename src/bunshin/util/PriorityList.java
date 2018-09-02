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

import java.util.*;
 
/**
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class PriorityList {

	private ArrayList list = new ArrayList();
	private ArrayList priorities = new ArrayList();

    //max properties
	private int indexMax = 0;
	private int max = 0;
		
	public void insert(Object elem, int priority) {
		
	  list.add(elem);	
	  int index = list.indexOf(elem);
	  priorities.add(index,new Integer(priority));
	  
	  if (priority>=max) {
	  	indexMax = index;
	  	max = priority;
	  }	  
	}
	
	public void update(Object elem) {
		
	  int index = list.indexOf(elem);
	  int priority = 0;
	  if (index<0) {
	  	list.add(elem);	
	    index = list.indexOf(elem);	  
	    priorities.add(index,new Integer(priority));
	  }
	  else {
	  	priority = ((Integer) priorities.get(index)).intValue() + 1;	  
	    priorities.set(index,new Integer(priority));
	  }
	  
	  if (priority>=max) {
	  	indexMax = index;
	  	max = priority;
	  	System.out.println("New max "+elem+" : "+max);
	  }
	  
	}
	
	public Object peak() {
	  return list.get(indexMax);		
	}
	
	public int getMax() {
	  return max;	
	}
	
	public void clear() {
	 
	  indexMax = 0;
	  max = 0;
		
	  priorities.clear();	
	  list.clear();	
	  
	}
	
	
	
}