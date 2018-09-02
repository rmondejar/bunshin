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

/**
*		Wrapper Exception class to different Exceptions
*   It simply stores the incoming Exception and provides a method for obtaining the *real* exception.
*/

public class StorageException extends Exception {
	private Exception ex;
	
	
	public StorageException(Exception ex)
	{
		super(ex.getMessage());
		this.ex = ex;
	}

	/**
	*		Return the Exception produces in the notification service.
	* @return Exception the Exception occured in the underlying Notificacion Service.
	*/
	
	public Exception getException()
	{
		return ex;
	}

}