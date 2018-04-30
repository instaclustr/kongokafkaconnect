package com.instaclustr.kongokafkaconnect;

/*
 * Top level main program to run Kongo simulation
 * 
 *
 * This version of Kongo is for Kafka connector experiments.
 * Make violation messages into topics. Don't need consumer classes, just producer as events written to the topic will be picked up by connector.
 * 
 */


public class KafkaRun
{
	
	 public static void main(String[] args)
	 {
	     System.out.println("Welcome to the Instaclustr KONGO IoT Demo Application for Kafka!");
	     
	     if (Simulate.oneTopic)
	    	 	System.out.println("MODE = one topic with single consumer");
	     else
	    	 	System.out.println("MODE = multiple topics, oner per location, each Goods subscribed to location topic");

	     // if we create > 1 consumer make sure that the number of partitions is >= numSensorConsumers
	     
	     if (Simulate.oneTopic)
	     {
	    	 	// how many concurrent Kafka sensor consumers
	    	 	// NOTE Only makes sense to increase consumers if partitions is > 1!
	    	 	int numSensorConsumers = 1;
	    	 	System.out.println("Starting numSensorConsumers=" + numSensorConsumers + " subscribed to " + Simulate.kafkaSensorTopicBase);
	    	 	
	    	 	for (int i=0; i < numSensorConsumers; i++) {
	    	 		SensorConsumer sensorConsumer = new SensorConsumer(Simulate.kafkaSensorTopicBase);
	    	 		sensorConsumer.start();
	    	 	}
	     }
	     // else code in Simulate makes SensorGroupConsumer objects, 1 per Goods.
	     // Note that this won't work unless both Unload and load consumers are enabled!
	     
	     if (Simulate.kafkaRFIDConsumerOn)
	     {
	    	 	// create RFID consumers here
	    	 	// create single topic to ensure event order
	    	 	System.out.println("Started kafka RFIDEventConsumer on topic " + Simulate.rfidTopic);

	    	 	RFIDEventConsumer rfidConsumer = new RFIDEventConsumer(Simulate.rfidTopic);
	    	 	rfidConsumer.start();
	     }
	     
	     // Start simulation which produces event streams
	    	 Simulate simulateThread = new Simulate();
	    	 simulateThread.start();
	    	 
	    	 System.out.println("Kafka Run has started Kongo!");
	 }
}