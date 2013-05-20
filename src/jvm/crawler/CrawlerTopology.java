package crawler;

// Standard Storm Imports
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import tutorial.bolts.LoadParserBolt;
import tutorial.spouts.UptimeSpout;

import crawler.schemes.SimpleURLScheme;
// Crawler pieces
import crawler.spouts.*;
import crawler.bolts.*;

import com.rabbitmq.client.AMQP;
import com.rapportive.storm.spout.AMQPSpout;
import com.rapportive.storm.amqp.ExclusiveQueueWithBinding;
import com.rapportive.storm.amqp.SharedQueueWithBinding;

public class CrawlerTopology {
		
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
			
//		builder.setSpout("urls", new MemoryURLSpout(), 1);
		builder.setSpout("urls", new AMQPSpout(
				"localhost", AMQP.PROTOCOL.PORT, "guest", "guest", "/", 
				new SharedQueueWithBinding("crawler", "amq.direct", "crawler"), 
				new SimpleURLScheme()
			), 5);
		builder.setBolt("uniqueurls", new UniqueUrlBolt(), 5).fieldsGrouping("urls", new Fields("url"));
		builder.setBolt("downloader",  new HTTPURLRetrieverBolt(), 5).shuffleGrouping("uniqueurls");
		builder.setBolt("linkfinder", new LinkFinderBolt(), 5).shuffleGrouping("downloader");
//		builder.setBolt("linksubmitter", new MemoryLinkSubmitterBolt(), 1).shuffleGrouping("linkfinder");
		builder.setBolt("solrindexer", new SolrIndexerBolt(), 5).shuffleGrouping("downloader");
		builder.setBolt("linkfilter", new LinkSubmissionFilterBolt(), 1).fieldsGrouping("linkfinder", new Fields("url"));
		builder.setBolt("queuelinksubmitter",  new RabbitMQLinkSubmitterBolt(), 1).shuffleGrouping("linkfilter");
		builder.setBolt("linkcounter", new MongoDBLinkCounterBolt(), 1).fieldsGrouping("linkfinder", new Fields("url"));
		
		Config conf = new CrawlerConfig();
		conf.setDebug(true);
		
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown(); 
	}
}
