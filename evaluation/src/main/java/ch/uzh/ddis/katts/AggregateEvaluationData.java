package ch.uzh.ddis.katts;

public class AggregateEvaluationData {
	
	
	public static void main(String[] args) {

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}
		
		String googleSpreadSheetName = null;
		String googleUsername = null;
		String googlePassword = null;
		String pathToEvaluationFolder = "";
		String jobName = null;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--google-spreadsheet-name")) {
				i++;
				googleSpreadSheetName = args[i];
			} else if (args[i].equalsIgnoreCase("--google-username")) {
				i++;
				googleUsername = args[i];
			} else if (args[i].equalsIgnoreCase("--google-password")) {
				i++;
				googlePassword = args[i];
			} else if (args[i].equalsIgnoreCase("--job-name")) {
				i++;
				jobName = args[i];
			} 
			else if (args[i].startsWith("--")) {
				i++;
				// Unknown parameter, ignore it.
			}
			else {
				pathToEvaluationFolder = args[i];
			}
		}
		
		Aggregator aggregator = new Aggregator(pathToEvaluationFolder, jobName);
		
		if (googleSpreadSheetName != null && googleUsername != null && googlePassword != null) {
			GoogleSpreadsheet spreadsheet = new GoogleSpreadsheet(googleUsername, googlePassword, googleSpreadSheetName);
			aggregator.setGoogleSpreadsheet(spreadsheet);
		}
		
		
		aggregator.aggregateMessagePerHost();
		
		
	}
	
	
	public static String getUsageMessage() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("Usage: AggregateEvaluationData path-to-evaluation-folder").append("\n\n");
		return builder.toString();
	}
}
