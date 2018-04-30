package com.instaclustr.kongokafkaconnect;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

// at time LOAD goodsKey at warehouseKey onto truckKey
public class RFIDLoadEvent
{
	long time; 
	String warehouseKey;
	String goodsKey;
	String truckKey;
	
	public RFIDLoadEvent(long time, String goodsKey, String warehouseKey, String truckKey)
	{
		this.time = time;
		this.goodsKey = goodsKey;
		this.warehouseKey = warehouseKey;
		this.truckKey = truckKey;
	}
	
	public void print()
	{
		System.out.println(time + ", " + goodsKey + ", " + warehouseKey + ", " + truckKey);
	}
	
	
	public RFIDLoadEvent()
	{
	}
	
	@Subscribe
	public void rfidLoadEvent(RFIDLoadEvent event)
	{
		// at time LOAD goodsKey at warehouseKey onto truckKey
		System.out.println("rfidLoadEvent handler: " + event);
		
		String locFrom = event.warehouseKey;
		String locTo = event.truckKey;
		
		// to move the goods need to get the Goods object itself
		// where is Goods now? claims to be at warehouseKey
		EventBus topicFrom = Simulate.topics.get(locFrom);
		
		// what if it's not?
		if (topicFrom == null)
			System.out.println("RFID LOAD EXCEPTION can't find topic for location " + locFrom);
		
		// This requires access to the global list of allGoods - nasty?!
		Goods goods = Simulate.allGoods.get(event.goodsKey);
		
		// unregister goods from warehouse topic location
		if (topicFrom != null) System.out.println("unregister from warehouse " + topicFrom.identifier());

		try
		{
			topicFrom.unregister(goods);
		}
		catch (Exception e)
		{
			System.out.println("LOAD EVENT Violation: Goods= " + event.goodsKey + " could not be loaded from warehouse location " + locFrom);
		}
		
		// change location
		// goods.locKey = locTo;
		EventBus topicTo = Simulate.topics.get(locTo);
		System.out.println("register with truck " + topicTo.identifier());
		
		topicTo.register(goods);
			
		System.out.println("post co-location event to " + topicTo.identifier());
		
		ColocatedCheckEvent ce = new ColocatedCheckEvent(time, event.goodsKey, event.truckKey);
		
		// send this event to the location topics as all goods at the truck location will need to be check their rules.
		topicTo.post(ce);			
		
	}
}
