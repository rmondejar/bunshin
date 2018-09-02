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

import java.io.*;
import java.util.*;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import org.jdom.*; 
import org.jdom.input.*; 

import bunshin.util.*;
import rice.p2p.commonapi.*;

/**
 *
 * @author Ruben Mondejar
 */
public class XMLStorage implements StorageManager {

	private final String XML_FILE_SUFFIX = ".xml";
    private String ROOT_PATH = "storage";	
    
	
	private Hashtable<String, String> paths = new Hashtable<String, String>(); //context --> path	
	private Hashtable<String, Bucket> memTable = new Hashtable<String, Bucket>();//key-->value
	
	XStream xstream = new XStream(new DomDriver());
	SAXBuilder builder = new SAXBuilder(); 
	
	private long max = 0;
	public XMLStorage() {}

	public XMLStorage (String root_path) {
		ROOT_PATH = root_path;
		loadInfoDir();
		/*
		try {
			loadAll();
		} catch (JDOMException e) {		
			e.printStackTrace();
		}*/
	}

	public XMLStorage (String root_path, long max_size) {

		ROOT_PATH = root_path;
		max = max_size;
		loadInfoDir();
		/*
		try {
			loadAll();
		} catch (JDOMException e) {			
			e.printStackTrace();
		}
		*/
	}

	public XMLStorage (long max_size)  {
		max = max_size;
		loadInfoDir();
		/*
		try {
			loadAll();
		} catch (JDOMException e) {			
			e.printStackTrace();
		}
		*/
	}

	public void init (Properties prop) {
	
		String root_path = (String)prop.get ("BUNSHIN_STORAGE_ROOT_DIR");
		if (root_path!=null) {

			ROOT_PATH = root_path+ROOT_PATH;
			//System.out.println("ROOT_PATH : "+ROOT_PATH);
		}
		loadInfoDir();
		/*
		try {
			loadAll();
		} catch (JDOMException e) {			
			e.printStackTrace();
		}
		*/

	}

	public void setPath(String context, String path) {
		
		String oldPath = paths.get(context);       
       
        if (oldPath!= null && !oldPath.equals(path)) {
        
		  File f = new File(oldPath);
		  File f2 = new File(path);

		  if (f!=null) 	{    
		     
		    //exists new directory?
   	        //if (!f.exists())  
		    
            //move all files to new directory??
  		    f2.renameTo(f);		             

  		  }
  		  else f2.mkdirs();   		 

		}   
		else {
			File f2 = new File(path);
			f2.mkdirs();
		}
		
	    paths.put(context,path);
		saveInfoDir();
	}

	public void add(String context, Bucket newBucket) throws StorageException {

		if (memTable.containsKey(context)) {
			Bucket mem = memTable.get(context);						
			mem.add(newBucket);	 
			memTable.put(context,mem); 
		}
		else {						
			memTable.put(context,newBucket);	
		}

		if (max==0 || memSize()<max) {
			try {
				saveAll(context);
			} 
			catch(Exception ex) 
			{
				throw new StorageException(ex);
			}
		}
	}
   
    
	public void put(String context, Bucket newBucket) throws StorageException 	{		

		memTable.put(context,newBucket);	
         
		if (max==0 || memSize()<max) {
			try {
				removeAll(context);
				saveAll(context);
			} 
			catch(Exception ex) {
				throw new StorageException(ex);
			}
		}
	}
    
	public void write(String context, Id key, Serializable value) throws StorageException {  

		Bucket mem;
		if (memTable.containsKey(context))	{
			mem = memTable.get(context);			  
		}
		else {
			mem = new Bucket();
		}
		
		mem.overwrite(key,value);		
		memTable.put(context,mem);

		if (max==0 || memSize()<max) {
			try {
				save(context,key);
			} 
			catch(IOException ex) {
				throw new StorageException(ex);
			}
		}
	}
	

	public Serializable remove(String context, Id key) throws StorageException {
		
		Serializable obj = null;
		if (memTable.containsKey(context)) {
			
			Bucket mem = memTable.get(context);
			obj = mem.remove(key);		
		
			if (max==0 || memSize()<max) {
				try {					
					delete(context,key);					
				} 
				catch(IOException ex) {
					throw new StorageException(ex);
				}
			} 
		}
		return obj;
	}

		    
	public synchronized Serializable extract(String context, Id key) { 	
		
		Serializable value = null;
		
		if (memTable.containsKey(context)) {
			
			Bucket mem = memTable.get(context);	
			value = mem.extract(key);		
		}
		if (value==null) {
			try {
				value = load(context,key);
			} catch (JDOMException e) {			
				e.printStackTrace();
			}
		}
		return value;
	}  
  
	public synchronized Bucket getBucket(String context) {
		
		loadContext(context);
		
		if (memTable.containsKey(context))	{
		  
		  Bucket mem1 = memTable.get(context);
		  Bucket mem = (Bucket) mem1.clone();
		  
		  Iterator<Id> it = mem.keySet().iterator();	
			
	      while(it.hasNext()) {
		    Id key = (Id) it.next();
		    Serializable values = mem.extract(key);
		    if (values!=null) {
		      mem.overwrite(key,values);
		    }		    
		  }	
			
		  return mem;
		}
		return null;
	}   

	public synchronized Hashtable<String, Bucket> getBuckets() {
        
        Hashtable<String, Bucket> buckets = new Hashtable<String, Bucket>();
        Iterator<String> it = memTable.keySet().iterator();
        
        while (it.hasNext()) {
          String context = it.next();
          Bucket mem = getBucket(context);  
          buckets.put(context,mem);
        }              
        
		return buckets;
	}   
	

	public boolean exists(String context, Id key) 	{
		if (memTable.containsKey(context)) {
			
			Bucket mem = memTable.get(context);
			return mem.containsKey(key);	    	  
		}
		return false;
	}





    private void saveInfoDir() {    	
	
	  try {	  				
		File f = new File(ROOT_PATH);
		
		if (!f.exists()) f.mkdirs();

		f = new File(ROOT_PATH+"/"+"infoDir");
		f.createNewFile();
		PrintWriter pw = new PrintWriter(f);		
		xstream.toXML(paths,pw);
	
		//ObjectOutputStream oos = new ObjectOutputStream(out);
		//oos.writeObject(paths);
		pw.close();
	  }
	  catch (IOException ex) {
		ex.printStackTrace();
	  }
    }
    
    private void loadInfoDir() {    	
	    	
	    try {
	    		
		File dir = new File(ROOT_PATH);
		
		if (dir.exists()) {
		
		  File f = new File(ROOT_PATH+"/"+"infoDir");
		  if (f.exists()) {
				
		    FileInputStream in = new FileInputStream(f);		    
		    //paths =(Hashtable) ois.readObject();
		    paths =(Hashtable<String, String>) xstream.fromXML(in);
		    in.close();
		    if (paths==null) paths = new Hashtable<String, String>();
		  } 
		  
		  //temp dirs
		  File[] subdirs = dir.listFiles();
		  //for each subdir
		  for (int i=0;i<subdirs.length;i++) {
		    if (subdirs[i].isDirectory())
		     // paths.put(subdirs[i].getName(),subdirs[i].getName());
		      paths.put(subdirs[i].getName(),subdirs[i].getPath());
		     //System.out.println("name : "+subdirs[i].getName()+" path : "+subdirs[i].getPath());
		  }
		  		  
		}  
		
		}
		catch (IOException ex) 
		{
			ex.printStackTrace();
		}
		   
    }
    
	private void save(String context,Id key) throws IOException {

		//File dir = new File(ROOT_PATH);
        String dir = paths.get(context);		

        if (dir==null) dir = ROOT_PATH+"/"+context;			
		
		File f = new File(dir);
		
		if (!f.exists()) f.mkdirs();
		
		Bucket mem = memTable.get(context);
		//Serializable values = mem.extract(key);
		Serializable values = mem.remove(key);
		
		Pair pair = new Pair(key,values);
		String name = ((rice.pastry.Id)key).toStringFull();
		f = new File(dir+"/"+name+".xml");
		f.createNewFile();
				
		PrintWriter pw = new PrintWriter(f);		
	    xstream.toXML(pair, pw);
		pw.close();

	}

	private void delete(String context, Id key) throws IOException {

		//File dir = new File(ROOT_PATH);
		String dir = paths.get(context);
	    
	    if (dir==null) dir = ROOT_PATH+"/"+context;			
		
		File f = new File(dir);
		
		if (!f.exists()) f.mkdirs();

		String name = ((rice.pastry.Id)key).toStringFull();
		f = new File(dir+File.separator+name+XML_FILE_SUFFIX);
		f.delete();
		//System.out.println("removing file "+name);
		
	}

	public void removeBucket(String context) throws StorageException {		
		
		memTable.remove(context);	
        try {
		  removeAll(context);
	    }  catch(IOException ex) 	{
	      throw new StorageException(ex);
        }
	}

	private void removeAll(String context) throws IOException {
		
		String dir = paths.get(context);	
	
	    if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		File f = new File(dir);		

		if (f.exists()) 	
		{
			File[] files = f.listFiles();
			for (int i=0;i<files.length;i++) {
				files[i].delete();
			}
			f.delete();
		}
	}

	private void saveAll(String context) throws IOException, JDOMException {

			String dir = paths.get(context);
						
			if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		    File f = new File(dir);
		
		    if (!f.exists()) f.mkdirs();
		

			Bucket mem = memTable.get(context);
			Collection<Id> c = mem.keySet();
			Iterator<Id> it2 = c.iterator();
			
			while(it2.hasNext()) {
				Id key = (Id)it2.next();				
				Serializable values = mem.extract(key);
				Pair pair = new Pair(key,values);
				String name = ((rice.pastry.Id)key).toStringFull();
				f = new File(dir+File.separator+name+XML_FILE_SUFFIX);
				f.createNewFile();
				PrintWriter pw = new PrintWriter(f);
				xstream.toXML(pair, pw);
				pw.close();
			}
		
	}

	private void saveAll() throws IOException, JDOMException {

		Iterator<String> it = paths.keySet().iterator();
		//for each directory
		while(it.hasNext())	{

			String context = it.next();
			String dir = paths.get(context);
			
            if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		    File f = new File(dir);
		
   		    if (!f.exists()) f.mkdirs();
		

			Bucket mem = memTable.get(context);
			Collection<Id> c = mem.keySet();
			Iterator<Id> it2 = c.iterator();
			
			while(it2.hasNext()) {
				
				Id key = (Id)it2.next();				
				Serializable values = mem.extract(key);
				Pair pair = new Pair(key,values);
				String name = ((rice.pastry.Id)key).toStringFull();
				f = new File(dir+File.separator+name+XML_FILE_SUFFIX);
				f.createNewFile();
				//FileOutputStream out = new FileOutputStream(f);
				PrintWriter pw = new PrintWriter(f);
				
				xstream.toXML(pair, pw);
				pw.close();
			}
		}
	}
	
	private Serializable load(String context, Id key) throws JDOMException 	{
		
		Serializable value = null;
		
		    //Bucket mem = memTable.get(context); 
		    String dir = paths.get(context);
			//System.out.println("context dir :"+dir);
			
			if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		    String name = ((rice.pastry.Id)key).toStringFull();
		    File f = new File(dir+File.separator+name+XML_FILE_SUFFIX);						
		    						
			  //ObjectInputStream ois = xstream.createObjectInputStream(dr);
			  //System.out.println("TRYING :"+name);  
			  try {
				  
				  FileInputStream in = new FileInputStream(f);
			    //Pair pair=(Pair) ois.readObject();
				Pair pair=(Pair) xstream.fromXML(in);  
			    //key = pair.getKey();
			    value= pair.getValues();
						  
			    //mem.overwrite(key,values);
			    //memTable.put(context,mem); 
			    
						  
			   } catch(Exception e) {
				  //System.out.println("CRASHING :"+name); 
			     //e.printStackTrace();
			   }		 
			 //System.out.println("load file "+name);
			 //System.out.println("mem"+mem+"key"+key+","+" values"+values);	 			
		
			   return value;		
	}

	@Override
	public void loadContext(String context) {		

			Bucket mem = memTable.get(context);
			if (mem==null) mem = new Bucket();
			String dir = paths.get(context);
			//System.out.println("context dir :"+dir);
			
			if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		    File f = new File(dir);
		
		    if (!f.exists()) f.mkdirs();
		
			if (f.isDirectory())  	{
				try 
				{
					File[] files = f.listFiles();
					for (int i=0;i<files.length;i++) {
										
					     //System.out.println("Reading "+files[i].getName());				
						 FileInputStream in = new FileInputStream(files[i]);
						//DomReader dr = new DomReader((Document) builder.build(f));
						 //ObjectInputStream ois = new ObjectInputStream (in);
						 												
						String name = files[i].getName(); 
						if (name.endsWith(XML_FILE_SUFFIX)) {						
						
						 
						  //ObjectInputStream ois = xstream.createObjectInputStream(dr);
						  //System.out.println("TRYING :"+name);  
						  try {
						    //Pair pair=(Pair) ois.readObject();
							Pair pair=(Pair) xstream.fromXML(in);  
						    Id key = pair.getKey();
						    Serializable values= pair.getValues();
						  
						    mem.overwrite(key,values);
						    memTable.put(context,mem);
						  
						  } catch(Exception e) {
							  //System.out.println("CRASHING :"+name); 
							  //e.printStackTrace();
						  }
						  in.close();
						  //System.out.println("load file "+name);
						  //System.out.println("mem"+mem+"key"+key+","+" values"+values);
						}  
					}
				}
				catch (IOException ex) 
				{
					//ex.printStackTrace();
				}
			}		
	}
	
	private void loadAll() throws JDOMException 	{
        		
		Iterator<String> it = paths.keySet().iterator();
		//for each directory
		while(it.hasNext())	{

			String context = it.next();
			Bucket mem = memTable.get(context);
			if (mem==null) mem = new Bucket();
			String dir = paths.get(context);
			//System.out.println("context dir :"+dir);
			
			if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
		    File f = new File(dir);
		
		    if (!f.exists()) f.mkdirs();
		
			if (f.isDirectory())  	{
				try 
				{
					File[] files = f.listFiles();
					for (int i=0;i<files.length;i++) {
										
					     System.out.println("Reading "+files[i].getName());				
						 FileInputStream in = new FileInputStream(files[i]);
						//DomReader dr = new DomReader((Document) builder.build(f));
						 //ObjectInputStream ois = new ObjectInputStream (in);
						 												
						String name = files[i].getName(); 
						if (name.endsWith(XML_FILE_SUFFIX)) {						
						
						 
						  //ObjectInputStream ois = xstream.createObjectInputStream(dr);
						  //System.out.println("TRYING :"+name);  
						  try {
						    //Pair pair=(Pair) ois.readObject();
							Pair pair=(Pair) xstream.fromXML(in);  
						    Id key = pair.getKey();
						    Serializable values= pair.getValues();
						  
						    mem.overwrite(key,values);
						    memTable.put(context,mem);
						  
						  } catch(Exception e) {
							  //System.out.println("CRASHING :"+name); 
							  e.printStackTrace();
						  }
						  in.close();
						  //System.out.println("load file "+name);
						  //System.out.println("mem"+mem+"key"+key+","+" values"+values);
						}  
					}
				}
				catch (IOException ex) 
				{
					ex.printStackTrace();
				}
			}
			//else no files...			
		}
		//System.out.println("Size "+memSize());		
	}

	private long memSize() 	{
        
		long size=0;

		Iterator<String> it = paths.keySet().iterator();
		//for each directory
		while(it.hasNext())	{

			String context = it.next();
			String dir = paths.get(context);
            
		    if (dir==null) dir = ROOT_PATH+File.separator+context;			
		
   		    File f = new File(dir);
		
		    if (!f.exists()) f.mkdirs();			
		    
			if (f.isDirectory()) {
				File[] files = f.listFiles();
				//for each file
				for (int i=0;i<files.length;i++) {
					//System.out.println("dir :"+f.getName()+" file "+files[i].getName());
				
					size+=files[i].length();
				}
			}
		}
		return size;
	}

	public boolean isFull() {
		return (memSize()<max);
	}	
	
	@Override
	public Collection<String> getContexts() {	
		Set<String> contexts = new HashSet<String>();
		contexts.addAll(memTable.keySet());	
	    File root = new File(ROOT_PATH);
	    if (root!=null) {
	      for(File f : root.listFiles()) {
	    	if (f.isDirectory()) {
	    		contexts.add(f.getName());
	    	}
	      }
	    }  
	    return contexts;
	}

	@Override
	public int getContextSize(String context) {
		// 
		Bucket b = memTable.get(context);
		Set<String> ids = new HashSet<String>();
		if (b!=null) {
		  for(Id id : b.keySet()) {
			ids.add(id.toStringFull());
		  }
		}			
	    File root = new File(ROOT_PATH+File.separator+context);
	    for(File f : root.listFiles()) {
	    	String name = f.getName();
	    	if (name.endsWith(XML_FILE_SUFFIX)) {
	    		String id = name.substring(0,name.length()-XML_FILE_SUFFIX.length());
	    		ids.add(id);
	    	}
	    }
	    return ids.size();
	}

	
	  
	
}