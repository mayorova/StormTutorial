package crawler.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class RabbitMQLinkSubmitterBolt extends BaseRichBolt {
	OutputCollector _collector;
	private final static String QUEUE_NAME = "crawler";
	Connection connection;
	Channel channel;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			connection = factory.newConnection();
			channel = connection.createChannel();
		}
		catch (Exception e)
		{
			throw new RuntimeException("Unable to connect to RabbitMQ: " + e.getMessage());
		}
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getString(0);
		try
		{
			channel.queueDeclare(QUEUE_NAME, true, false, false, null);
			channel.basicPublish("amq.direct", QUEUE_NAME, null, url.getBytes());			
		}
		catch (Exception e)
		{
			throw new RuntimeException("Failed to publish to RabbitMQ: " + e.getMessage());
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// This bolt doesn't emit any tuples :)
	}
	
	public void cleanup() {
		try
		{
			connection.close();			
		} catch(Exception e)
		{
			throw new RuntimeException("Exception shutting down RabbitMQ connection: " + e.getMessage());
		}
	} 

}
