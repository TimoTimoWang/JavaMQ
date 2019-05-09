package MQ;

import java.nio.ByteBuffer;

/**
 * 消息解码工具类，用来从缓存读取一个消息并返回
 * @author YINLI
 *
 */

public class MessageDecode {
	
	static final MessageDecode messageDecoder = new MessageDecode();
	static final byte INT = 1;
	static final byte LONG = 2;
	static final byte DOUBLE = 3;
	static final byte STRING = 4;
	
	/**
	 * 从缓存in中读取一个消息并返回
	 * @param in
	 * @return
	 */
	public static ByteMessage getMessage(ByteBuffer in) {
		//构造一个头部
		KeyValue header = new KeyValue();
		//初始化头部键值对数目
		int kvNum = 0;
		//从缓存中读取头部键值对数目，如果抛出异常说明没有消息可读了，直接返回null
		try {
			kvNum = in.getInt();
		}catch (Exception e) {
			return null;
		}
		
		//逐对读取键值对并放到header里
		for (int i=0; i < kvNum; i++) {
			getKV(header,in);
		}
		
		//读取body长度，构造相应字节数组并从缓存里读取body
		int lenBody = in.getInt();
		byte[] body = new byte[lenBody];
		in.get(body);
		
		//构造消息并返回
		ByteMessage msg = new ByteMessage(body);
		msg.setHeaders(header);
		
		return msg;
	}
	
	
	/**
	 * 从缓存in中读取一对键值对放到header里
	 * @param header
	 * @param in
	 */
	private static void getKV(KeyValue header, ByteBuffer in) {
		//读取key的标志位
		byte keyByte = in.get();
		//根据标志来确定key的值
		String key = new String();
		switch(keyByte) {
		case 1: key = MessageHeader.BORN_HOST;break;
		case 2: key = MessageHeader.BORN_TIMESTAMP;break;
		case 3: key = MessageHeader.MESSAGE_ID;break;
		case 4: key = MessageHeader.PRIORITY;break;
		case 5: key = MessageHeader.RELIABILITY;break;
		case 6: key = MessageHeader.SCHEDULE_EXPRESSION;break;
		case 7: key = MessageHeader.SEARCH_KEY;break;
		case 8: key = MessageHeader.SHARDING_KEY;break;
		case 9: key = MessageHeader.SHARDING_PARTITION;break;
		case 10: key = MessageHeader.START_TIME;break;
		case 11: key = MessageHeader.STOP_TIME;break;
		case 12: key = MessageHeader.STORE_HOST;break;
		case 13: key = MessageHeader.STORE_TIMESTAMP;break;
		case 14: key = MessageHeader.TIMEOUT;break;
		case 15: key = MessageHeader.TOPIC;break;
		case 16: key = MessageHeader.TRACE_ID;break;
		}
		//初始化value
		Object value = new Object();
		//读取value标志位
		int valueType = in.get();
		//读取value值
		switch(valueType) {
		case INT:
			value = in.getInt();header.put(key,value);break;
		case LONG:
			value = in.getLong();header.put(key, value);break;
		case DOUBLE:
			value = in.getDouble();header.put(key, value);break;
		case STRING:
			int strLen = in.getInt();
			byte[] valueBytes = new byte[strLen];
			in.get(valueBytes);
			value = new String(valueBytes);
			header.put(key, value);
		}
	}
	
}
