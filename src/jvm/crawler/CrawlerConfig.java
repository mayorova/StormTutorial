package crawler;

import backtype.storm.Config;

public class CrawlerConfig extends Config {
	// Set the topology message time out to be five minutes, 
	// gives page retrieval a chance to run.
	public static String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "300";
}
