package bunshin.messaging;

import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;

import java.io.IOException;

public class BunshinDeserialize implements MessageDeserializer {

	public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
	        switch (type) {
	            case MessageType.CACHE_MESSAGE:
	        		return new CacheMessage(buf);
	        
	        	case MessageType.GET_MESSAGE: 
	    			return new GetMessage(buf);
	    	
	        	case MessageType.LEAVE_MESSAGE:
	        		return new LeaveMessage(buf);
		    
	        	case MessageType.PUT_ACK_MESSAGE:
	        		return new PutAckMessage(buf);		
	    	
	        	case MessageType.PUT_MESSAGE:
	        		return new PutMessage(buf);
		   	    		
	        	case MessageType.REMOVE_ACK_MESSAGE:
	        		return new RemoveAckMessage(buf);
		    	
	    	   	case MessageType.REMOVE_MESSAGE:
	        		return new RemoveMessage(buf);
	        		
	        	case MessageType.REMOVE_REPLICA_MESSAGE:
	        		return new RemoveReplicaMessage(buf);
		    	   	
	        	case MessageType.REPLICA_ACK_MESSAGE:
	        		return new ReplicaAckMessage(buf);
	    	
	        	case MessageType.REPLICA_MESSAGE:
	        		return new ReplicaMessage(buf);
		    	
	        	case MessageType.PING_MESSAGE:
	        		return new PingMessage(buf);

	      }
			return null;    
	}
	
}
