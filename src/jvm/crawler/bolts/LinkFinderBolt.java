package crawler.bolts;

import java.util.Iterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LinkFinderBolt extends BaseRichBolt {
	OutputCollector _collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Document doc = Jsoup.parse(input.getString(1), input.getString(0));
		Iterator<Element> linkIterator = doc.select("a").iterator();
		while(linkIterator.hasNext())
		{
			Element link = linkIterator.next();
			String url = link.attr("abs:href");
			if (url != null && url.length() > 4 && url.substring(0, 4).toLowerCase().equals("http"))
			{
				_collector.emit(input, new Values(url));
			}
		}
		
		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

}
