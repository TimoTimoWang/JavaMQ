package MQ;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

/**
 * 生产者，每个生产者线程之间除了初始化创建目录不存在锁
 */
public class Producer {

	//缓存大小为64M，当缓存剩余空间不到256K的时候写到文件里
	static final int BUFF_SIZE = 8*1024*1024;
	static final int REMAIN_SIZE = 256*1024;
	
	HashMap<String, RandomAccessFile> fouts = new HashMap<>();
	HashMap<String, FileChannel> fcs = new HashMap<>();
	HashMap<String, ByteBuffer> buffs = new HashMap<>();
	//Path是放文件的目录
	String Path = "data/data/";
	File base = new File(Path);
	String topic;
	//用num标记当前线程
	int num;
	
	/**
	 * 传入body和topic生成一个message然后返回
	 * @param topic
	 * @param body
	 * @return
	 */
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new ByteMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    
    /**
     * 构造函数
     */
    public Producer() {
    	
    	//如果基目录不存在则创建，因为共有四个线程所以需要上锁
    	synchronized(this) {
    		if (!base.exists()) {
    			base.mkdir();
    		}
    	}
    	//标记此线程
		num = MessageStore.store.threadNum.addAndGet(1);
    	
	}
	
    /**
     * 发送msg消息
     * @param msg
     * @throws IOException
     */
    public void send(ByteMessage msg) throws IOException{
        
    	//如果消息为空直接返回
    	if (msg == null)
    		return;
    	//先取它的topic
    	topic = msg.headers().getString(MessageHeader.TOPIC);
    	//如果文件输出流里面还没有这个topic文件就new一个对应的文件输出流
    	//同时开一个管道，为这个文件分配一个对应的堆外缓存
    	if (!fouts.containsKey(topic)) {
    		fouts.put(topic, new RandomAccessFile(new File(Path + num + "_" +topic.toString()),"rw"));
			fcs.put(topic, fouts.get(topic).getChannel());
			buffs.put(topic, ByteBuffer.allocateDirect(BUFF_SIZE));
    	}
    	
    	//如果此topic对应的缓存剩余小于指定数就翻转一下把缓存存到文件里面取，然后清缓存
    	if (buffs.get(topic).remaining()<REMAIN_SIZE) {
    		//缓存flip一下
    		buffs.get(topic).flip();
    		//把缓存里的东西写到文件里去
			fcs.get(topic).write(buffs.get(topic));
			//清空缓存
			buffs.get(topic).clear();

       	}
    	//将消息放到缓存里去
		MessageEncode.storeMessage(msg, buffs.get(topic));        
    }
    
    /**
     * 发送完所有消息调用，将缓存中的东西都写到文件中去
     * @throws Exception
     */
    public void flush()throws Exception{

    	System.out.println(1);
    	//将所有缓存都flip一下准备写文件
    	buffs.forEach((k,v) -> {
    		v.flip();
    	});
    	//所有缓存写入文件，然后关闭管道
    	fcs.forEach((k,v) -> {
			try {
				v.write(buffs.get(k));
				v.close();
			} catch (IOException e) {
    			e.printStackTrace();
			}
		});
    	//关闭文件流
    	fouts.forEach((k,v) -> {
    		try {
    			v.close();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	});
             
    }
    

}
