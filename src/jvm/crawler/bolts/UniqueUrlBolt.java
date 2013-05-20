package crawler.bolts;

import java.util.LinkedList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UniqueUrlBolt extends BaseRichBolt {
	OutputCollector _collector;
	LinkedList<String> urls = new LinkedList<String>();
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String url = input.getString(0);
		if (!urls.contains(url))
		{
			urls.add(url);
			_collector.emit(input, new Values(url));
		}
		
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}
}
