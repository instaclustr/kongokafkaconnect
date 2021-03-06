package com.instaclustr.kongokafkaconnect;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes RFID Event objects from/to strings.
 */

public class RFIDLoadEventSerializer implements Closeable, AutoCloseable, Serializer<RFIDLoadEvent>, Deserializer<RFIDLoadEvent> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    @Override
    public byte[] serialize(String s, RFIDLoadEvent rfid) {
    		String line =
    				rfid.time + ", " +
    				rfid.warehouseKey + ", " +
    				rfid.goodsKey + ", " +
    				rfid.truckKey;
        return line.getBytes(CHARSET);
    }
	
    @Override
    public RFIDLoadEvent deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            long time = Long.parseLong(parts[i++]);
            String warehouseKey = parts[i++];
            String goodsKey = parts[i++];
            String truckKey = parts[i++];
            
            // 	public RFIDLoadEvent(long time, String goodsKey, String warehouseKey, String truckKey)
            return new RFIDLoadEvent(time, goodsKey, warehouseKey, truckKey);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}