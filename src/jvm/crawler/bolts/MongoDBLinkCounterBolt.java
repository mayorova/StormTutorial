package crawler.bolts;

import java.util.Map;

import org.jongo.MongoCollection;
import org.jongo.Jongo;

import com.mongodb.DB;
import com.mongodb.Mongo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MongoDBLinkCounterBolt extends BaseRichBolt {
	OutputCollector _collector;
	DB db;
	Jongo jongo;
	MongoCollection urls;


	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		try
		{
			db = new Mongo().getDB("stormtutorial");
			jongo = new Jongo(db);
			urls = jongo.getCollection("urls");
		}
		catch (Exception e)
		{
			throw new RuntimeException("Unable to connect to MongoDB: " + e.getMessage());
		}
	}

	@Override
	public void execute(Tuple input) {
		urls.update("{url: '#'}", input.getString(0)).upsert().multi().with("{$inc: {hits: 1}}");
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// The link counter just pushes into MongoDB.
	}

}
