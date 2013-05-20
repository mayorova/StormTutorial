package crawler.bolts;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import crawler.spouts.MemoryURLSpout;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LinkSubmissionFilterBolt extends BaseRichBolt {
	OutputCollector _collector;
	String[] safeDomains = {"pasamio.id.au"};
	List<String> crawledUrls = new ArrayList<String>();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			URL link = new URL(input.getString(0));
			boolean submit = false;
			for(int i = 0; i < safeDomains.length; i++)
			{
				if (link.getHost().contains(safeDomains[i]))
				{
					submit = true;
					break;
				}
			}
			
			if (submit && !crawledUrls.contains(input.getString(0)))
			{
				crawledUrls.add(input.getString(0));
				_collector.emit(input, new Values(input.getString(0)));
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException("Unable to process " + input.getString(0) + ": " + e.getMessage());
		}
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

}
