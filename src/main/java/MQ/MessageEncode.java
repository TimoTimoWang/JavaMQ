package MQ;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * 消息编码工具类，用来将消息编码然后写入缓存
 * @author YINLI
 *
 */
public class MessageEncode {

	static final MessageEncode messageEncoder = new MessageEncode();
	static final byte INT = 1;
	static final byte LONG = 2;
	static final byte DOUBLE = 3;
	static final byte STRING = 4;
	
	
	/**
	 * 将msg存到out缓存里
	 * @param msg
	 * @param out
	 */
	public static void storeMessage(ByteMessage msg, ByteBuffer out) {
		//取msg的头部hashmap
		HashMap<String, Object> kvs = msg.headers().getMap();
		//先写入k-v的对数
		out.putInt(kvs.size());
		//再写入每对k-v
		kvs.forEach((k,v) -> {
			putKV(k,v,out);
		});
		//然后写入body的长度
		out.putInt(msg.getBody().length);
		//然后写入body
		out.put(msg.getBody());
	}
	
	/**
	 * 将key-value键值对写入out缓存里
	 * @param key
	 * @param value
	 * @param out
	 */
	private static void putKV(String key, Object value, ByteBuffer out) {
		//将key转换为对应的byte并写入
		switch(key) {
		case MessageHeader.BORN_HOST:out.put((byte) 1);break;
		case MessageHeader.BORN_TIMESTAMP:out.put((byte) 2);break;
		case MessageHeader.MESSAGE_ID:out.put((byte) 3);break;
		case MessageHeader.PRIORITY:out.put((byte) 4);break;
		case MessageHeader.RELIABILITY:out.put((byte) 5);break;
		case MessageHeader.SCHEDULE_EXPRESSION:out.put((byte) 6);break;
		case MessageHeader.SEARCH_KEY:out.put((byte) 7);break;
		case MessageHeader.SHARDING_KEY:out.put((byte) 8);break;
		case MessageHeader.SHARDING_PARTITION:out.put((byte) 9);break;
		case MessageHeader.START_TIME:out.put((byte) 10);break;
		case MessageHeader.STOP_TIME:out.put((byte) 11);break;
		case MessageHeader.STORE_HOST:out.put((byte) 12);break;
		case MessageHeader.STORE_TIMESTAMP:out.put((byte) 13);break;
		case MessageHeader.TIMEOUT:out.put((byte) 14);break;
		case MessageHeader.TOPIC:out.put((byte) 15);break;
		case MessageHeader.TRACE_ID:out.put((byte) 16);
		}
		
		
		//判断value类型并写入缓存
		if (value.getClass()==Integer.class) {
			out.put(INT);
			out.putInt((int) value);
		}
		if (value.getClass()==Long.class) {
			out.put(LONG);
			out.putLong((long) value);
		}
		if (value.getClass()==Double.class) {
			out.put(DOUBLE);
			out.putDouble((double) value);
		}
		if (value.getClass()==String.class) {
			out.put(STRING);
			out.putInt(((String)value).length());
			out.put(((String)value).getBytes());
		}	
	}
	
}
