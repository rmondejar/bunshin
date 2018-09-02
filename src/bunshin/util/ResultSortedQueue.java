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
 * Queue by the number of matches of the elems
 *
 * @author Ruben Mondejar
 * @author Jordi Pujol
 *
 */ 
public class ResultSortedQueue {
    
    private Vector keyQueue,valueQueue;        
    private int[] limits; 
    private Set matches;
    
    //   0   -> limits[0] -> limits[1] -> ... -> limits[limits.length]
    // [ 0 matches ) [ 1 match  )          [  limits.length matches )
    
    public ResultSortedQueue(Set matches) {
      
      keyQueue = new Vector();
      valueQueue = new Vector();
      this.matches = matches;
      limits = new int[matches.size()+1];
      
    }
    
    private int matchesCalc(Set elems) {
    	
      int num = 0;
      
      Iterator it = matches.iterator();
      while (it.hasNext()) {
      	if (elems.contains(it.next())) {
      	  num++;
      	}
      }      
      return num;
    }
    
    private int getMatches(int pos) {
      
      boolean found = false;
      
      for (int i=0;i<limits.length-2;i++) {
        if (pos>limits[i] && pos<limits[i+1]) {
       	  return i;	
        }
      } 
      if (!found) return limits.length-1; 	
      else return -1;
    }
    
    
    public void add(Object key, Set elems) {
              
       int num = matchesCalc(elems);
       int pos = limits[num];
       for (int i=num+1;i<limits.length;i++) limits[i]++;
       
       keyQueue.add(pos,key);
       valueQueue.add(pos,elems);
            
    }
    
    public void delete(Object key) {
       
       int pos = keyQueue.indexOf(key);
       keyQueue.removeElementAt(pos); 
       valueQueue.removeElementAt(pos); 
       
       int limit = getMatches(pos);
       limits[limit]--;              
    } 
    
    public Iterator iterator() {
      return keyQueue.iterator();
    }   
    
    public String toString() {
      String s = "\nResultSortedQueue ("+keyQueue.size()+" elements) :\n";
      for (int i=keyQueue.size()-1; i>=0; i--) {
      	int num_matches = getMatches(i);
      	Object key =  keyQueue.get(i);
      	Object value =  valueQueue.get(i);  
      	int num = matchesCalc((Set)value);
        s+="Key : "+key+" ("+num+" matches) --> "+value+"\n";     
      }
      return s;
    }
	
	/**
	 * Return specific iterator for the ResultSortedQueue.
	 */
	public ResultIterator getItems()
	{
		return new ResultIterator();
	}
	
	/**
	 * Return a specific iterator for the ResultSortedQueue.
	 */
	public class ResultIterator
	{
		private int index = 0;
		
		/**
		 * Builds a new iterator for the actual
		 */
		public ResultIterator()
		{
			index = keyQueue.size()-1;
		}
		
		/**
		 * Return true if has more items on results.
		 */
		public boolean hasNext() 
		{
			return index>=0;
		}
		
		/**
		 * Return a new Item with the next value.
		 */
		public Item next()
		{
			Item item = new Item(index);
			index--;
			return item;
		}
	}
	
	/** 
	 * Element within the ResultSortedQueue.
	 */
	public class Item {
		private Object value      = null;
		private int numberMatches = 0;
		private Set matches       = null;
		
		/**
		 * Builds an element with all required information of an element
		 * within the ResultSortedQueue.
		 * @index index Index within ResultSortedQueue.
		 */
		public Item(int index)
		{
			this.value         = keyQueue.get(index);			
			this.matches       = (Set)valueQueue.get(index);
			this.numberMatches = matchesCalc(matches);
		}
		
		/**
		 * Returns the related value of this item.
		 * @return The related value of this item within ResultSortedQueue.
		 */
		public Object getValue()
		{
			return value;
		}
		
		/**
		 * Sets/Updates the reference to the value of this item.
		 * @param value New value for this item.
		 */
		public void setValue(Object value)
		{
			this.value = value;
		}
		
		/**
		 * Returns the number of matches of this item, into the search string.
		 * @return The number of matches of this item, into the search string.
		 */
		public int getNumberOfMatches()
		{
			return numberMatches;
		}
		
		/**
		 * Returns the set of matches (set of strings)
		 * @return The set of matches.
		 */
		public Set getAllMatches()
		{
			return matches;
		}
		
	}
}
