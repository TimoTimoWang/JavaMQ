package MQ;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;

/**
 * 消费者
 */

public class Consumer {

	//缓存大小和规定剩余量
	static final int BUFF_SIZE = 8*1024*1024;
	static final int REMAIN_SIZE = 256*1024;
	
	String queue;
	String path = "data/data/";
	File base = new File(path);
	File[] currentFiles;
	//用files来存放所有和此消费者关联的文件，index来标记当前读的文件
	ArrayList<File> files = new ArrayList<>();
	int index = 0;
	FileChannel fc = null;
	ByteBuffer buff = ByteBuffer.allocate(BUFF_SIZE);
	
	//绑定及初始化files数组
	public void attachQueue(String queueName, Collection<String> t) throws Exception {
		
		if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
		queue = queueName;
		
		//遍历每个topic
		t.forEach((topic) -> {
			//先获取所有以该topic为后缀的文件
			currentFiles = base.listFiles(new FilenameFilter() {	
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(topic);
				}
			});
			//如果一个也没有就说明没有这个topic的消息
			//如果有就把文件添加到文件数组中
			if (currentFiles.length>0) {
				for (File currentFile : currentFiles) {
					files.add(currentFile);
				}
			}
		});
		
	}
	
	
	public ByteMessage poll() throws IOException {
		//如果管道为空且索引没到最后说明刚开始读，如果管道为空但索引到最后说明没有文件可以读了
		if (fc == null && index != files.size()) {
			//取下一个通道在这里即第一个通道
			fc = nextChannel();
			//读满缓存
			readTillFull();
			//缓存flip准备读消息
			buff.flip();
		}
		
		//如果发现缓存剩的不多了并且当前还有管道就读满缓存
		//如果缓存剩的不多但是当前管道为null，这个null不可能表示刚开始，因为上一个条件已经判断过刚开始
		//所以只有可能是读完所有文件了，这样就不用管缓存，直接继续读消息直到读到null
		if (buff.remaining()<REMAIN_SIZE && fc != null) {
			//缓存compact一下
			buff.compact();
			//将缓存读满
			readTillFull();
			//缓存flip准备读消息
			buff.flip();
		}
		
		//从缓存读消息并返回
		ByteMessage msg = MessageDecode.getMessage(buff);
		return msg;
	}
	
	
	//获取下一个管道
	private FileChannel nextChannel() throws IOException {
		//如果当前管道不为空，则关闭当前管道
		if (fc != null) 
			fc.close();
		//如果没到最后一个文件就返回index文件的管道，同时index加一
		if (index < files.size()) {
			return new FileInputStream(files.get(index++)).getChannel();
		}
		//如果到了最后一个文件，就返回null
		else
			return null;
	}
	
	//这个函数在只有在初始化的时候或者剩余不多的时候才调用
	private void readTillFull() throws IOException {
		
		//先把当前的通道里的数据读进缓存
		if (fc != null)
			fc.read(buff);
		//当前管道为空就是读完所有文件了，直接返回
		else 
			return;
		//如果读完当前通道缓存还没满,就让管道指向下一个文件，然后递归调用此方法
		//如果满了，此时当前管道维持不变，然后递归栈直接返回
		if (buff.hasRemaining()) {
			fc = nextChannel();
			readTillFull();
		}
		
	}
	
	
}
