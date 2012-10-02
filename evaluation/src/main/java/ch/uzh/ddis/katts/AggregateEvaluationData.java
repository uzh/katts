package ch.uzh.ddis.katts;

public class AggregateEvaluationData {
	
	
	public static void main(String[] args) {

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}
		
		String pathToEvaluationFolder = args[0];
		
		Aggregator aggregator = new Aggregator(pathToEvaluationFolder);
		aggregator.aggregateMessagePerHost();
		
		
	}
	
	
	public static String getUsageMessage() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("Usage: AggregateEvaluationData path-to-evaluation-folder").append("\n\n");
		return builder.toString();
	}
}
