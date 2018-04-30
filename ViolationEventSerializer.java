package com.instaclustr.kongokafkaconnect;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes ViolationEvent objects from/to strings.
 */

public class ViolationEventSerializer implements Closeable, AutoCloseable, Serializer<ViolationEvent>, Deserializer<ViolationEvent> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    //@Override
    public byte[] oldserialize(String s, ViolationEvent violationEvent) {
    	
    		String line =
    				violationEvent.time + ", " +
    				violationEvent.doc + ", " +
    				violationEvent.goodsTag + ", " +
    				violationEvent.goodsCats + ", " + 
    				violationEvent.location + ", " + 
    				violationEvent.violation;		
        return line.getBytes(CHARSET);
    }
    
    // JSON version!
    @Override
    public byte[] serialize(String s, ViolationEvent violationEvent) {
    	
    		String goods = violationEvent.goodsTag; 
    		String error = violationEvent.violation;

    		String line =
    				"{\"goods\": \"" +
    				goods +
    				"\", " + 
    				"\"error\": \"" +
    				error +
    				"\"}";

    		String lineOld =
    				violationEvent.time + ", " +
    				violationEvent.doc + ", " +
    				violationEvent.goodsTag + ", " +
    				violationEvent.goodsCats + ", " + 
    				violationEvent.location + ", " + 
    				violationEvent.violation;		

        return line.getBytes(CHARSET);
    }
	
    // deserialize not used as events consumed by Kafka connector.
    @Override
    public ViolationEvent deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            long time = Long.parseLong(parts[i++]);
            String doc = parts[i++];
            String goodsTag = parts[i++];
            String goodsCats = parts[i++];
            String location = parts[i++];
            String violation = parts[i++];

            return new ViolationEvent(time, doc, goodsTag, goodsCats, location, violation);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}