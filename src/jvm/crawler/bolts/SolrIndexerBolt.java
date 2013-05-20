package crawler.bolts;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.Map;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SolrIndexerBolt extends BaseRichBolt {
	OutputCollector _collector;
	String solrExtractPath = "http://localhost:8983/solr/crawler/update/extract?commit=true";
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		MessageDigest md;
		String md5;

		// This here is what we in PHP would do as "md5sum($variable)":
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(input.getString(0).getBytes());
			byte[] messageDigest = md.digest();
			
			// Convert to hex string
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < messageDigest.length; i++) {
			    sb.append(Integer.toHexString(0xff & messageDigest[i]));
			}
			md5 = sb.toString();
		} catch (Exception e)
		{
			throw new RuntimeException("Was unable to hash the URL: " +  e.getMessage());
		}
		
		HttpResponse<String> response = Unirest.post(solrExtractPath + "&literal.id=" + md5 + "&literal.url=" + input.getString(0))
				  .header("accept", "application/json")
				  .body(input.getString(1))
				  .asString();

		_collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Don't return any result.
	}

}
