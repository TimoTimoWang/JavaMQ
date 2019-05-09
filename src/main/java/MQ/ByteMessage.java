package MQ;

/**
 * 字节消息接口
 *
 */
public class ByteMessage {

    private KeyValue headers = new KeyValue();
    private byte[] body;

    public void setHeaders(KeyValue headers) {
        this.headers = headers;
    }

    public ByteMessage(byte[] body) {
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public KeyValue headers() {
        return headers;
    }

    public ByteMessage putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    public ByteMessage putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    public ByteMessage putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    public ByteMessage putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }
    
    public ByteMessage putHeaders(String key, Object obj) {
    	headers.put(key, obj);
    	return this;
    }

	@Override
	public String toString() {
		return new String(body);
		
	}
    
    
}