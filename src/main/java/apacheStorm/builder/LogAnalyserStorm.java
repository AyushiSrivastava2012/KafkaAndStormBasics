package apacheStorm.builder;

import java.io.FileNotFoundException;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import apacheStorm.Bolts.CallLogCreatorBolt;
import apacheStorm.spouts.FakeCallLogReaderSpout;


public class LogAnalyserStorm {
	public static void main(String[] args) throws Exception{
		//read property file
		


		//
		//		Class loadedMyClass = classLoader.loadClass(classPathForTestJava);
		//		Constructor constructor = loadedMyClass.getConstructor();
		//		Object myClassObject = constructor.newInstance();
		//		Method method = loadedMyClass.getMethod(methodName);
		//		System.out.println("Invoked method name: " + method.getName());
		//		method.invoke(myClassObject);


		//Create Config instance for cluster configuration
			Config config = new Config();
		config.setDebug(true);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

		builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt(),10)
		.shuffleGrouping("call-log-reader-spout");
		 
		//		builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(),2)
		//		.fieldsGrouping("call-log-creator-bolt", new Fields("call"));

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
		Thread.sleep(10000);

		//Stop the topology
		//
		//		Utils.sleep(1000);

		//		// kill the topology
		//		final KillOptions killOptions = new KillOptions();
		//		killOptions.set_wait_secs(0);
		//		cluster.killTopologyWithOpts("LogAnalyserStorm", killOptions);
		//
		//		// wait until the topology is removed from the cluster
		//		while (topologyExists("LogAnalyserStorm",cluster)) {
		//			// avoid cpu overuse
		//			Utils.sleep(1000);
		//		}
		//
		//		// for some reason I have to wait to be sure topology is stopped and local cluster can be shutdown
		//		Utils.sleep(5000);
		//		cluster.shutdown();
	}

	//	private final static boolean topologyExists(final String topologyName, LocalCluster cluster) {
	//
	//		// list all the topologies on the local cluster
	//		final List<TopologySummary> topologies = cluster.getClusterInfo().get_topologies();
	//
	//		// search for a topology with the topologyName
	//		if (null != topologies && !topologies.isEmpty()) {
	//			final List<TopologySummary> collect = topologies.stream().filter(p -> p.get_name().equals(topologyName)).collect(Collectors.toList());
	//			if (null != collect && !collect.isEmpty()) {
	//				return true;
	//			}
	//		}
	//		return false;
	//	}
}
