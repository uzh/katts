package ch.uzh.ddis.katts;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import ch.uzh.ddis.katts.evaluation.Evaluation;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;

/**
 * This class runs the given query locally. The query must be serialized as XML.
 * 
 * @author Thomas Hunziker
 *
 */
public class RunXmlQueryLocally {
	
	public static final String RUN_TOPOLOGY_LOCALLY_CONFIG_KEY = "katts_run_topology_locally";
	
	/**
	 * This method builds the topology from the XML file. This method expected a XML file which contains the query
	 * serialized in XML.
	 * 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0 || args[0] == null) {
			throw new Exception("The first parameter must be the path to the xml file with the query.");
		}
		
		String path = args[0];
		Query query = Query.createFromFile(path).validate().optimize();
		
		int numberOfWorkers = 20;
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(numberOfWorkers);
		
		TopologyBuilder builder = new TopologyBuilder(conf);
		builder.setQuery(query);
		builder.setParallelismByNumberOfProcessors(numberOfWorkers);
		
		
		List<String> hookClass = new ArrayList<String>();
		hookClass.add("ch.uzh.ddis.katts.monitoring.TaskMonitor");
		conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hookClass);
		
		conf.put(RUN_TOPOLOGY_LOCALLY_CONFIG_KEY, true);
		
		// Log every 15 seconds the Java Virtual Machine properties
		conf.put(VmMonitor.RECORD_INVERVAL, 30);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		
//		for (int i=0; i<100; i++) {
//			Map state = cluster.getState();
//			Object zkConnection = state.get(Keyword.intern(Symbol.create(":zookeeper")));
//			
//			System.out.println(zkConnection);
//			for (Object key : state.keySet()) {
//				Object value = state.get(key);
//				System.out.println(key.toString() + " = " + value);
//			}
//			Thread.sleep(1000);
//		}
		//:daemon-conf = {"dev.zookeeper.path" "/tmp/dev-storm-zookeeper", "topology.tick.tuple.freq.secs" nil, "topology.fall.back.on.java.serialization" true, "topology.max.error.report.per.interval" 5, "zmq.linger.millis" 0, "topology.skip.missing.kryo.registrations" true, "ui.childopts" "-Xmx768m", "storm.zookeeper.session.timeout" 20000, "nimbus.reassign" true, "topology.trident.batch.emit.interval.millis" 50, "nimbus.monitor.freq.secs" 10, "java.library.path" "/usr/local/lib:/opt/local/lib:/usr/lib", "topology.executor.send.buffer.size" 1024, "storm.local.dir" "storm-local", "supervisor.worker.start.timeout.secs" 120, "topology.enable.message.timeouts" true, "nimbus.cleanup.inbox.freq.secs" 600, "nimbus.inbox.jar.expiration.secs" 3600, "drpc.worker.threads" 64, "topology.worker.shared.thread.pool.size" 4, "nimbus.host" "localhost", "storm.zookeeper.port" 2000, "transactional.zookeeper.port" nil, "topology.executor.receive.buffer.size" 1024, "transactional.zookeeper.servers" nil, "storm.zookeeper.root" "/storm", "supervisor.enable" true, "storm.zookeeper.servers" ["localhost"], "transactional.zookeeper.root" "/transactional", "topology.acker.executors" 1, "topology.transfer.buffer.size" 1024, "topology.worker.childopts" nil, "drpc.queue.size" 128, "worker.childopts" "-Xmx768m", "supervisor.heartbeat.frequency.secs" 5, "topology.error.throttle.interval.secs" 10, "zmq.hwm" 0, "drpc.port" 3772, "supervisor.monitor.frequency.secs" 3, "topology.receiver.buffer.size" 8, "task.heartbeat.frequency.secs" 3, "topology.tasks" nil, "topology.spout.wait.strategy" "backtype.storm.spout.SleepSpoutWaitStrategy", "topology.max.spout.pending" nil, "storm.zookeeper.retry.interval" 1000, "topology.sleep.spout.wait.strategy.time.ms" 1, "nimbus.topology.validator" "backtype.storm.nimbus.DefaultTopologyValidator", "supervisor.slots.ports" [6700 6701 6702 6703], "topology.debug" false, "nimbus.task.launch.secs" 120, "nimbus.supervisor.timeout.secs" 60, "topology.message.timeout.secs" 30, "task.refresh.poll.secs" 10, "topology.workers" 1, "supervisor.childopts" "-Xmx256m", "nimbus.thrift.port" 6627, "topology.stats.sample.rate" 0.05, "worker.heartbeat.frequency.secs" 1, "topology.acker.tasks" nil, "topology.disruptor.wait.strategy" "com.lmax.disruptor.BlockingWaitStrategy", "nimbus.task.timeout.secs" 30, "storm.zookeeper.connection.timeout" 15000, "topology.kryo.factory" "backtype.storm.serialization.DefaultKryoFactory", "drpc.invocations.port" 3773, "zmq.threads" 1, "storm.zookeeper.retry.times" 5, "topology.state.synchronization.timeout.secs" 60, "supervisor.worker.timeout.secs" 30, "nimbus.file.copy.expiration.secs" 600, "drpc.request.timeout.secs" 600, "storm.local.mode.zmq" false, "ui.port" 8080, "nimbus.childopts" "-Xmx1024m", "storm.cluster.mode" "local", "topology.optimize" true, "topology.max.task.parallelism" nil}
//		Evaluation.collectAndStoreStats("katts-evaluation-lorenz", 
//										"katts.evaluation@gmail.com", 
//										"kattsevaluationongdocs", 
//										"test", "localhost", 6627);
}

}
