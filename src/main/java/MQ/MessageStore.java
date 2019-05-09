package MQ;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个工具类，只是用来存线程数目
 */
public class MessageStore {

	static final MessageStore store = new MessageStore();
	
	AtomicInteger threadNum = new AtomicInteger(0);

}
