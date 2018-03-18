package apacheStorm.spouts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FakeCallLogReaderSpout implements IRichSpout{

	//Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;
	private boolean completed = false;
	//Create instance for TopologyContext which contains topology data.
	private TopologyContext context;
	//Create instance for Random class.
	private Random randomGenerator = new Random();
	private Integer idx = 0;


	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void activate() {
		// TODO Auto-generated method stub

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		if(this.idx <= 1000) {
			List<String> mobileNumbers = new ArrayList<String>();
			mobileNumbers.add("1234123401");
			mobileNumbers.add("1234123402");
			mobileNumbers.add("1234123403");
			mobileNumbers.add("1234123404");

			Integer localIdx = 0;
			while(localIdx++ < 100 && this.idx++ < 1000) {
				String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));

				while(fromMobileNumber == toMobileNumber) {
					toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
				}

				Integer duration = randomGenerator.nextInt(60);
				this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
			}
		}

	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.context = context;
		this.collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("from", "to", "duration"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
