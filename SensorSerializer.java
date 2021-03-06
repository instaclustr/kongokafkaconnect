package com.instaclustr.kongokafkaconnect;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;

/**
 * (De)serializes Sensor objects from/to strings.
 */

public class SensorSerializer implements Closeable, AutoCloseable, Serializer<Sensor>, Deserializer<Sensor> {
	
    public static final Charset CHARSET = Charset.forName("UTF-8");

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    @Override
    public byte[] serialize(String s, Sensor sensor) {
    		String line =
    				sensor.time + ", " +
    				sensor.doc + ", " +
    				sensor.tag + ", " +
    				sensor.metric + ", " + 
    				sensor.value;		
        return line.getBytes(CHARSET);
    }
	
    @Override
    public Sensor deserialize(String topic, byte[] bytes) {
        try {
            String[] parts = new String(bytes, CHARSET).split(", ");

            int i = 0;
            long time = Long.parseLong(parts[i++]);
            String doc = parts[i++];
            String tag = parts[i++];
            String metric = parts[i++];
            double value = Double.parseDouble(parts[i++]);

            return new Sensor(time, doc, tag, metric, value);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void close() {

    }
}