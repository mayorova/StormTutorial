package crawler.bolts;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HTTPURLRetrieverBolt extends BaseRichBolt {
	OutputCollector _collector;
	public static Logger LOG = org.apache.log4j.Logger.getLogger(HTTPURLRetrieverBolt.class);
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try
		{
			URL targetUrl = new URL(input.getString(0));
			URLConnection conn = targetUrl.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String content = "";
			String inputLine;
			while ((inputLine = in.readLine()) != null)
			{
				content += "\r\n" + inputLine;
			}
			in.close();
			_collector.emit(input, new Values(input.getString(0), content));
		}
		catch (Exception e)
		{
			LOG.log(Priority.ERROR, "Unable to retrieve URL '" + input.getString(0) + "', message: " + e.getMessage());
		}
		_collector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url", "content"));
	}

}
