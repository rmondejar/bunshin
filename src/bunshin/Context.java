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

/**
*		This interface represents a naming context, which consists of a set of name-to-object bindings
*   These constants will be used for connecting to different DHT services.
*/

public class Context {
	
	public static final String HOST				= "BUNSHIN_HOST";
	
	public static final String PORT 			= "BUNSHIN_PORT";	
	
	public static final String FACTORY 			= "BUNSHIN_FACTORY";
	
	public static final String PROTOCOL 		= "BUNSHIN_PROTOCOL";
	
	public static final String ID_APPLICATION 	= "BUNSHIN_ID_APPLICATION";		
	
	public static final String STORAGE_MANAGER 	= "BUNSHIN_STORAGE_MANAGER";
	
	public static final String REPLICA_FACTOR 	= "BUNSHIN_REPLICA_FACTOR";
	
	public static final String CACHE 			= "BUNSHIN_CACHE";
	
	public static final String DEBUG 			= "BUNSHIN_DEBUG";
	
	public static final String REFRESH_TIME     = "BUNSHIN_REFRESH_TIME_SEC";
	
	public static final String TRUE             = "TRUE";
	
	public static final String FALSE            = "FALSE";

	public static  String DEFAULT_CONTEXT = "#unknown";
	
	public static  String DEFAULT_FIELD = null;
	
	/**
	   * REGISTRY_NAMING represents the  naming distributed object location and routing facility
	   */
	  public static final String REGISTRY_NAMING = "REGISTRY_NAMING";

	  /**
	   * REGISTRY_UNIQUE_URI represents the object URI identifier associated to an object
	   */
	  public static final String REGISTRY_UNIQUE_URI = "REGISTRY_UNIQUE_URI";

	  /**
	   * REGISTRY_CHILDREN refers to the name of the field containing children objects for any object
	   */
	  public static final String REGISTRY_CHILDREN = "REGISTRY_CHILDREN";

	  /**
	   * REGISTRY_PARENT refers to the parent of an object (in the naming hierarchy)
	   */
	  public static final String REGISTRY_PARENT = "REGISTRY_PARENT";
	
	/**
	   * Timeout constant. All timeouts will be consumed once TIMEOUT*DELAY milliseconds have been elapsed
	   */
	  public static final int TIMEOUT = 1000;

	  /**
	   * Default delay for synchronous calls
	   */
	  public static final int DELAY = 10;

	  /**
	   * Number of event retransmissions in case all timeouts have been consumed
	   */
	  public static final int RETRANSMISSIONS = 5;

	
}