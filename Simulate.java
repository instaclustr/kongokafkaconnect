package com.instaclustr.kongokafkaconnect;


/* Kongo IoT Application Simulation.
 * main() creates the world and calls loop to run the simulation for desired number of "hours" (turns).
 * loop does all the work, unloads Goods (generated RFID unload events), loads Goods (generate RFID load events), moves trucks, generates sensor stream events and checks for Goods/sensor rules violations.
 * 
 * Version 1.0: Paul Brebner, Instaclustr.com, February 2018
 * 
 * This is a simplistic stand-alone monolithic version which combines the simulation and rules checking, and is not particularly efficient or scalable. 
 * 
 * Version 2.0: Introduced real topics and events (using Guava EventBus)
 * 
 * Version 3.0: Introduced Kafka topics, producers and consumers.
 * 
 * Version 4.0: New Kafka topic for violation events, sent to Cassandra via connector. 
 */


import java.util.*;


import java.util.concurrent.ThreadLocalRandom;

import com.google.common.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.eventbus.SubscriberExceptionContext;

// Kafka imports

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Simulate extends Thread {
	
	// Kafka Producers
	// Need to have correct types here!	
	static KafkaProducer<String, Sensor> producerSensorEvents;
	
	static KafkaProducer<String, RFIDLoadEvent> producerRFIDLoadEvents;
	
	static KafkaProducer<String, RFIDUnloadEvent> producerRFIDUnloadEvents;
	
	// single topic rfid
	static KafkaProducer<String, RFIDEvent> producerRFIDEvents;
	
	
	static boolean kafkaSensorConsumerOn = true; 		// if true then pass sensor events to Kafka topic, which has a consumer that publishes event on correct topic.
	static boolean kafkaRFIDConsumerOn = true; 			// same for RFID events
	static boolean kafkaViolationTopicOn = true;			// if true write violations to Kafka topic.
	
	// topic names
	// kong2- has 3 partitions
	static String kafkaSensorTopicBase = "kong2-";
	static String unloadTopic = "kongo-unload";
	static String loadTopic = "kongo-load";
	// new topic for sensor and RFID violations to experiment with Kafka connect to Cassandra
	static String violationTopic = "kongo-violations";

	// single rfid topic version for both load and unload events using RFIDEvent
	static String rfidTopic = "kongo-rfid3";
	
	static boolean oneTopic = true; // oneTopic to rule them all or lots of topics per location with each Goods subscribed to different topic
	
	public Simulate()
	{
		
	}


	// Turn rules on or off for Goods movement control. Even with rules turned on there may be some violations as some things are random (e.g. truck accelerate and vibrations).	
	static boolean enforceTempRules = false;			// enforce goods temperature category rules for movements from/to trucks/warehouses
	static boolean enforceHazardousRules = false;	// enforce goods co-location rules when loading trucks
	static boolean checkGoods = true; 				// turn on/off co-location rules checking during simulation
	
	static boolean debug = false;
	static boolean verbose = false;
	
	// global data. all goods in the system (in trucks or warehouses)
	static HashMap<String, Goods> allGoods = new HashMap<String, Goods>();
	
	// all warehouses
	static HashMap<String, Warehouses> allWarehouses = new HashMap<String, Warehouses>();
	
	// all goods that are in warehouses
	static HashMap<String, String> goodsInWarehouses = new HashMap<String, String>();
	
	// all goods that are in trucks
	static HashMap<String, String> goodsInTrucks = new HashMap<String, String>();

	// all trucks
	static HashMap<String, Trucks> allTrucks = new HashMap<String, Trucks>();
	
	// trucks at each warehouse
	static HashMap<String, String> trucksAtWarehouses = new HashMap<String, String>();
	
	// topics using Event Bus. topics are warehouse or truck locations and receive all sensor events for those locations. Could have lots in theory.
	// each Goods object is subscribed to the topic where it's currently located.	
	static HashMap<String, EventBus> topics = new HashMap<String, EventBus>(1000);
	static EventBus rfidLoadTopic = null;
	static EventBus rfidUnloadTopic = null;

	// truckKey and keep1 are hacks used in Goods loading code in the simulation loop.	
	static String truckKey;
	
	// keep truck key first time, and then randomly 50% of time for others
	public static void keep1(String s)
	{
		if (truckKey == null || rand.nextBoolean())
			truckKey = s;	
	}
	
	static Random rand = new Random();
	
	// simulation loop, simulates Goods and Trucks movement for required number of rounds (hours)
	// assumes everything has been created already.
	public static void loop(int hours)
	{
		long t0 = System.currentTimeMillis();
		long totalEvents = 0;
		ProducerRecord r;
		
		// repeat for hours
		System.out.println("Simulation started");
		
		// loop the loop
		for (int time=0; time < hours; time++)
		{
			System.out.println("************** Time = " + time);
			
			// 1 UNLOAD trucks: move goods from trucks to warehouse where truck docked
			if (debug) System.out.println("Unloading goods from Trucks...");
			
			// Unload all Goods that are in trucks
			Iterator<String> it = goodsInTrucks.keySet().iterator();
			while (it.hasNext())
			{
			    String goodsKey = it.next();
			    String truckKey = goodsInTrucks.get(goodsKey);
			    			    
			    // find warehouse where the truck is
			    String warehouse = trucksAtWarehouses.get(truckKey);
			    // change location of goods to warehouse
			    goodsInWarehouses.put(goodsKey, warehouse);
			    if (debug) System.out.println("Unloaded " + goodsKey + " from " + truckKey + " at " + warehouse); 
			    
			    // generate UNLOAD RFID event: At time, unload goodsKey from trucksKey at warehouse
			    String s = time + " RFID " + warehouse + ": UNLOAD " +  goodsKey + " from " + truckKey;
			    if (verbose) System.out.println(s);
			    
			    // public RFIDUnloadEvent(long time, String goodsKey, String truckKey, String warehouseKey)
				RFIDUnloadEvent ule = new RFIDUnloadEvent(time, goodsKey, truckKey, warehouse);
				
				// 	public RFIDEvent(boolean t, long time, String goodsKey, String warehouseKey, String truckKey)
				RFIDEvent re = new RFIDEvent(false, time, goodsKey, warehouse, truckKey);
				
				try
				{
					r = new ProducerRecord(rfidTopic, re);
					producerRFIDEvents.send(r).get();
					
            			if (debug) System.out.println("Sent Kafka RFID UNLOAD event message " + r.toString());		
				} 
				catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
				
				if (!kafkaRFIDConsumerOn)
					rfidUnloadTopic.post(ule);
			    
			    totalEvents++;
			    it.remove();
			    // forget categories of loaded goods
			    Trucks t = allTrucks.get(truckKey);
			    t.resetCats();
			}
			
			// 2 LOAD Goods from warehouse to trucks currently docked at warehouse
			
			if (debug) System.out.println("Loading goods onto trucks");
			it = goodsInWarehouses.keySet().iterator();
			while (it.hasNext())
			{
				String goodsKey = it.next();
				String warehouseKey = goodsInWarehouses.get(goodsKey);
			    
			    // randomly decide if we want to load this good, check if there is a truck at the warehouse, load it, remove it from warehouse
				if (debug) System.out.println("Found goods " + goodsKey + " in " + warehouseKey + " try and load it? ");
			    if (rand.nextDouble() > 0.5)
			    {
			    	    truckKey = null;
			    		Map<String, String> map = trucksAtWarehouses;
			    	    map.entrySet()
			    	    .stream()
			    	    .filter(x -> x.getValue().equals(warehouseKey))
			    	    // hack need to pick 1 truck at random
			    	    .forEach(x -> keep1(x.getKey()));
			   
			    	   // if a truck was found...
			    	   if (truckKey != null)
			    	   {
			    		   if (debug) System.out.println("Found a truck at warehouse " + truckKey);
			    		   // load truck, remove goods from warehouse
			    		   Trucks t = allTrucks.get(truckKey);
			    		   Goods g = allGoods.get(goodsKey);
			    		   // can we load the Goods onto it?
			    		   boolean load = false;
			    		   load = g.allowedInTruck(t.categoriesOnBoard);
			    		
			    		   if (verbose && load) System.out.println("Goods allowed in truck, goods cats=" + g.categories + " no conflict with truck cats=" + t.categoriesOnBoard.allCategories());
			    		   else if (verbose && !load) System.out.println("Goods NOT ALLOWED in truck, goods cats=" + g.categories + " conflict with truck cats=" + t.categoriesOnBoard.allCategories());
			    		
			    		   // keep loading if we can load it or we don't care about enforcing rules
			    		   if (!enforceHazardousRules || enforceHazardousRules && load)
			    		   {
			    			   // check temperature control rules
			    			   if (load = g.truckTempRules(t))
			    				   if (verbose) System.out.println("Goods allowed on truck for temperature rules check");
			    				   else if (verbose) System.out.println("Goods NOT ALLOWED on truck for temperature rules check");
				    		
			    			   if (!enforceTempRules || enforceTempRules && load)
			    			   {
			    				   t.updateCategories(g);
			    				   goodsInTrucks.put(goodsKey, truckKey);
			    				   it.remove();
			    				   if (debug) System.out.println("Loading " + goodsKey + " onto " + truckKey);
				    	
			    				   // generate RFID LOAD event
			    				   String s = time + " RFID " + warehouseKey + ": LOAD " +  goodsKey + " onto " + truckKey;
			    				   if (verbose) System.out.println(s);
			    				   
			    				   // Order is ; public RFIDLoadEvent(long time, String goodsKey, String warehouseKey, String truckKey)
			    				   RFIDLoadEvent le = new RFIDLoadEvent(time, goodsKey, warehouseKey, truckKey);
			    				   
			    				   //  	public RFIDEvent(boolean t, long time, String goodsKey, String warehouseKey, String truckKey)
			    				   RFIDEvent re = new RFIDEvent(true, time, goodsKey, warehouseKey, truckKey);
			    				
			    				   
			    					try
			    					{
			    						r = new ProducerRecord(rfidTopic, re);
			    						producerRFIDEvents.send(r).get();
			    	            			if (debug) System.out.println("Sent Kafka RFID LOAD event message " + r.toString());		
			    					} 
			    					catch (InterruptedException | ExecutionException e) {
			    						e.printStackTrace();
			    					}
			    					
			    					if (!kafkaRFIDConsumerOn)
			    						 rfidLoadTopic.post(le);

			    				   totalEvents++;
			    			   }
			    		   }
			    	   }
			    }
			}
			
			// 3 Move TRUCKS
			
			// create shuffled list of warehouses to select destination warehouse from
			List<String> keyList = new ArrayList<String>(allWarehouses.keySet());
			Collections.shuffle( keyList );
			Iterator<String> randKeys = keyList.iterator();
			
			for (Map.Entry<String, String> entry : trucksAtWarehouses.entrySet())
			{
				String truckKey = entry.getKey();
				Trucks truck = allTrucks.get(truckKey);
				String currentLoc = entry.getValue();
				String destination = null;;
				
				// find a warehouse with a compatible temperature control
				Warehouses w = null;
				while (destination == null)
				{
					if (randKeys.hasNext())
						destination = randKeys.next();
					else
					{
						// else start again
						randKeys = keyList.iterator();
						destination = randKeys.next();
					}
					// does destination warehouse have compatible climate control to the truck temp control?
					w = allWarehouses.get(destination);
					if (!enforceTempRules || truck.canDeliverToWarehouse(w))
						break;
					// no good so keep looking
					else destination = null;
				}
				
				trucksAtWarehouses.put(truckKey, destination);
				
				if (verbose) System.out.println(time + " Truck " + truckKey + " temp cat=" + truck.tempRange + " moving from " + currentLoc + " to " + destination + " with temp cat=" + w.tempRange);
			}
			
			// 4 SENSOR stream, simple version, each warehouse and truck produce only out one value per sensor metric per location per hour
			// This is a very inefficient implementation as it checks rules for all goods in each truck every sensor event
			Sensor sensor;
		
			// Truck SENSOR stream
			// New version posts to topics
			// TODO No Goods objects will be subscribed to truck location topics at start!!!!
			for (String truckskey : allTrucks.keySet())
			{
				Trucks truck = allTrucks.get(truckskey);
				
				// find the topic corresponding to the location of the truck
				EventBus topic = topics.get(truckskey);
				if (topic != null)
				{
					try
					{
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "temp", truck.temp.randomTempInRange());
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					String tName;
					if (oneTopic)
						tName = kafkaSensorTopicBase;
					else
						tName = kafkaSensorTopicBase + truckskey;
					
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
					
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "humidity", randBetween(0, 100));
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
				
					// lux https://en.wikipedia.org/wiki/Lux range 0 - 100,000 (direct sunlight), 500 is office lighting, unit is lux
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "illuminance", randBetween(0, 100000));
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
					
					// acceleration, in standard gravities i.e. 0, 1, 100? Normal should be < 1g? fast car accel if about 0.5g
					// roller coaster is 3-4g
					// car https://physics.info/acceleration/ F1 could be up to 3g! A truck should be < 1g
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "acceleration", randBetween(0, 100));
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
					
					// vibration has amplitude and frequency (but sensors produce data for multiple frequencies!)
					// freq is Hz (0-100000), amp is ms-2 (0-?)
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "vibrationDisplacement", randBetween(0, 1000));
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
					
					sensor = new Sensor(time, "SENSOR TRUCK", truckskey, "vibrationVelocity", randBetween(0, 1000));
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					r = new ProducerRecord(tName, truckskey, sensor);
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA TRUCK event message " + r.toString());
					
					 } catch (InterruptedException | ExecutionException e) {
		                    e.printStackTrace();
					 }
					
					totalEvents += 6;
				}
			}
			
			// Warehouse SENSOR stream
			// New version posts sensor event to correct warehouse location topic	
			for (String warehouseKey : allWarehouses.keySet())
			{
				Warehouses warehouse = allWarehouses.get(warehouseKey);
				
				// find the topic corresponding to the warehouse location
				EventBus topic = topics.get(warehouseKey);
				
				if (topic != null)
				{				
					
					try
		            {
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "temp", warehouse.temp.randomTempInRange());
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					
					// 1 topic, warehouseKey as key, Kafka determines partition from hash of key
					// if using 1 topic per location assume that dynamic topic creation is enabled and use warehousekey as topic name
					
					String tName ;
					if (oneTopic)
						tName = kafkaSensorTopicBase;
					else
						tName = kafkaSensorTopicBase + warehouseKey;
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					
					producerSensorEvents.send(r).get();
            			if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
					
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "humidity", randBetween(0, 100));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
				
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "illuminance", randBetween(0, 100000));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
			
					// Nasty gases: ozone, particulate matter, toxic gas (Propane, Butane, LPG and Carbon Monoxide.), sulfur dioxide, and nitrous oxide
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "ozone", randBetween(0, 10000));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
				
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "particles", randBetween(0, 10000));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
					
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "toxicGas", randBetween(0, 10000));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
					
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "sulfurDioxide", randBetween(0, 10));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
	
					sensor = new Sensor(time, "SENSOR WAREHOUSE", warehouseKey, "nitrousOxides", randBetween(0, 10));
					
					if (!kafkaSensorConsumerOn)
						topic.post(sensor);
					
					r = new ProducerRecord(tName, warehouseKey, sensor);
					producerSensorEvents.send(r).get();
        				if (debug) System.out.println("Sent KAFKA SENSOR event message " + r.toString());
	
					// Kafka producer code              		
	                } catch (InterruptedException | ExecutionException e) {
	                    e.printStackTrace();
	                }
					
					totalEvents += 8;
				}
			}
		}
		
		System.out.println("Simulation ended");
		long t1 = System.currentTimeMillis();
		double duration = (t1 - t0)/1000.0;
		System.out.println("Simulation duration (s) = " + duration);
		double eventsSec = totalEvents/duration;
		System.out.println("Events = " + totalEvents + ". Rate (Events/s) = " + eventsSec);
	}

	public static double randBetween(double min, double max)
	{
		return (rand.nextDouble() * (max-min)) + min;
	}

	// new run method for use with Kafka in a thread
	 public void run()
	 {
		 createAndLoop();
	 }
	            
	public static void main()
	{
		createAndLoop();
	}
	            
	public static void createAndLoop() 
	{		
		// Parameters, how many Goods, warehouse locations and trucks, and hours to run simulation.
		int numGoods = 1000;
		int maxX = 10;
		int maxY = 10;
		int numWarehouses = maxX * maxY;
		int numTrucks = numWarehouses*2;
		int loops = 3;
	
		// CREATION
		// create random Goods in a hashMap
		for (int i = 0; i < numGoods; i++)
		{
			 Goods g = new Goods();
			 allGoods.put(g.tag, g);
	         String s = g.toStr();
	         System.out.println(s);
		}
	
		System.out.println("Goods created = " + numGoods);
	
		// create warehouses
		// Also create a topic per warehouse
		String aWarehouse = null;
	
		for (int x = 0; x < maxX; x++)
		{
			for (int y = 0; y < maxY; y++)
			{
				Warehouses w = new Warehouses(x, y);
				if (aWarehouse == null)
					aWarehouse = w.id;
				allWarehouses.put(w.id, w);
				String s = w.toStr();
				System.out.println(s);
				
				// create a topic for each warehouse
				EventBus eb = new EventBus(w.id);
				topics.put(w.id, eb);
			}
		}
	
		System.out.println("Warehouses created = " + maxX*maxY);
	
		// find warehouses with compatible environmental controls to put Goods in
		for (String goodskey : allGoods.keySet())
		{
			Goods g = allGoods.get(goodskey);
			boolean found = false;
			for (String warehouseKey: allWarehouses.keySet())
			{
				Warehouses w = allWarehouses.get(warehouseKey);
				if (g.warehouseTempRules(w))
				{
					goodsInWarehouses.put(goodskey, warehouseKey);
					found = true;
					break;
				}
			}
			// can't find anywhere just put goods in 1st warehouse
			if (!found) goodsInWarehouses.put(goodskey, aWarehouse);
		}
	
		// subscribe/register Goods to topics
		// Note that initially all goods are in warehouses, none are on trucks.
		for (Map.Entry<String, String> entry : goodsInWarehouses.entrySet())
		{
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println(key + " in " + value);
			
			Goods g = allGoods.get(key);
			EventBus topic = topics.get(value);
			topic.register(g);
			
			// subscribe Goods to Kafka sensor topics
			
			// else KafkaRun create a single topic and consumer and we use EventBus
			// this starts a thread per Goods Eeek.
			if (!oneTopic)
			{
				SensorGoodsConsumer sensorGoodsConsumer = new SensorGoodsConsumer(kafkaSensorTopicBase + value, g);
				sensorGoodsConsumer.start();			
			}
		}		
	
		System.out.println("Goods locations created");

		// Create Trucks to move Goods around
		// and a topic per truck
		for (int i = 0; i < numTrucks; i++)
		{
			 Trucks t = new Trucks();
			 t.resetCats();
			 allTrucks.put(t.id, t);
	         String s = t.toStr();
	         System.out.println(s);
	         
	         // create a topic for each truck
			EventBus eb = new EventBus(t.id);
			topics.put(t.id, eb);
		}
		
		System.out.println("Trucks created");
	
		// set Trucks locations to warehouses
		Iterator it = allWarehouses.keySet().iterator();
		String warehousekey;
		for (String truckskey : allTrucks.keySet())
		{
			if (it.hasNext())
				warehousekey = (String) it.next();
			else
				warehousekey = aWarehouse;	// else use first warehouse
			trucksAtWarehouses.put(truckskey, warehousekey);
		}
		
		for (Map.Entry<String, String> entry : trucksAtWarehouses.entrySet())
		{
			String key = entry.getKey();
			String value = entry.getValue();
			System.out.println(key + " at " + value);
		}
	
		System.out.println("Truck locations created");
		
		// create RFID Load and Unload event topics
		rfidLoadTopic = new EventBus("load");
		rfidUnloadTopic = new EventBus("unload");
		RFIDLoadEvent loadHandler = new RFIDLoadEvent();
		
		rfidLoadTopic.register(loadHandler);
		
		RFIDUnloadEvent unloadHandler = new RFIDUnloadEvent();
		rfidUnloadTopic.register(unloadHandler);
		
		// create Kafka producers
		Properties props = new Properties();        
	    props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put("client.id", "KongoSimulator");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    
	    String sensorSerializer = SensorSerializer.class.getName(); 
	    
	    props.put("value.serializer", sensorSerializer);
	    producerSensorEvents = new KafkaProducer<>(props);
	    
	    // RFID Load and Unload producers
	    props = new Properties();        
	    props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put("client.id", "KongoRFIDLoad");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    
	    String rfidLoadEventSerializer = RFIDLoadEventSerializer.class.getName(); 
	    props.put("value.serializer", rfidLoadEventSerializer);
	    producerRFIDLoadEvents = new KafkaProducer<>(props);
	    
	    props = new Properties();        
	    props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put("client.id", "KongoRFIDUnload");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");	    
	    String rfidUnloadEventSerializer = RFIDUnloadEventSerializer.class.getName(); 
	    
	    props.put("value.serializer", rfidUnloadEventSerializer);
	    producerRFIDUnloadEvents = new KafkaProducer<>(props);
	  
	    
	    // new single topic rfid
	    props = new Properties();        
	    props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
	    props.put("client.id", "KongoRFIDEvent");
	    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");	    
	    String rfidEventSerializer = RFIDEventSerializer.class.getName(); 
	    
	    props.put("value.serializer", rfidEventSerializer);
	    producerRFIDEvents = new KafkaProducer<>(props);
	
		// Run the simulation for loops hours
		loop(loops);
	}
}
