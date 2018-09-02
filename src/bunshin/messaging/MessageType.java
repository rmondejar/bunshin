package bunshin.messaging;

public interface MessageType {

	public static final short BUNSHIN_MESSAGE = -1;
	
	public static final short CACHE_MESSAGE = 0;

	public static final short GET_MESSAGE = 1;
		
	public static final short LEAVE_MESSAGE = 2;
	
	public static final short PUT_MESSAGE = 3;
	
	public static final short PUT_ACK_MESSAGE = 4;	
	
	public static final short REMOVE_ACK_MESSAGE = 5;
	
	public static final short REMOVE_MESSAGE = 6;
	
	public static final short REMOVE_REPLICA_MESSAGE = 7;
	
	public static final short REPLICA_ACK_MESSAGE = 8;
	
	public static final short REPLICA_MESSAGE = 9;
	
	public static final short PING_MESSAGE = 10;
	
}

