package apacheStorm.Bolts;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//Create a class CallLogCreatorBolt which implement IRichBolt interface
public class CallLogCreatorBolt implements IRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//Create instance for OutputCollector which collects and emits tuples to produce output
	private OutputCollector collector;


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	public void execute(Tuple tuple) {
		String from = tuple.getString(0);
		// String to = tuple.getString(1);
		//Integer duration = tuple.getInteger(2);
		System.out.println(from+"*************************************");

		if(from != null) {
			Properties prop = new Properties();
			String propFileName = "config.properties";

			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			InputStream inputStream = classLoader.getResourceAsStream(propFileName);

			if (inputStream != null) {
				try {
					prop.load(inputStream);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println("file not found.");
			}

			String classPathForTestJava= prop.getProperty("jarPath");
			String methodName=prop.getProperty("methodName");
			System.out.println("classpath********"+classPathForTestJava+" methodName ***"+methodName);

			File pluginsDir = new File(classPathForTestJava);

			try {
				String[] listOfFiles = classPathForTestJava.split(";");
				for(String fileName: listOfFiles) {
					if(fileName.compareTo(from) ==0 ) {
						Class<?> clazz = Class.forName(fileName, true, URLClassLoader.newInstance(new URL[] { pluginsDir.toURI().toURL() },
								Thread.currentThread().getContextClassLoader()));
						Constructor<?> constructor = clazz.getConstructor();
						Method method = clazz.getMethod("hello");
						method.invoke(constructor.newInstance());
					}	
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		//collector.emit(new Values(from));
		collector.ack(tuple);
	}


	public void cleanup() {}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("call"));
	}


	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}