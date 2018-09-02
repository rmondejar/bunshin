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

import java.io.*;
import rice.p2p.commonapi.*;

/**
 *
 * Pair Key/Values 
 *
 * @author Ruben Mondejar  <Ruben.Mondejar@estudiants.urv.es>
 */

public class Pair implements Serializable {
	
	private Id key;
	private Serializable values;
	
	public Pair(Id key, Serializable values) {
	  this.key=key;
	  this.values=values;
	}
	
	public Id getKey () {
		return key;
	}
	
	public Serializable getValues(){
		return values;
	}
}
