package ch.uzh.ddis.katts.evaluation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;

/**
 * This is the main class which, starts the evaluation process.
 * 
 * @author Thomas Hunziker
 * 
 */
public class Evaluation {

	public static void main(String[] args) throws IOException, TException {

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}

		String googleSpreadSheetName = null;
		String googleUsername = null;
		String googlePassword = null;
		String jobName = null;
		String nimbusHost = "localhost";
		int thriftPort = 6627;
		String evaluationOutPath = null;

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
			} else if (args[i].equalsIgnoreCase("--nimbus-host")) {
				i++;
				nimbusHost = args[i];
			} else if (args[i].equalsIgnoreCase("--thrift-port")) {
				i++;
				thriftPort = Integer.valueOf(args[i]);
			} else if (args[i].equalsIgnoreCase("--job-name")) {
				i++;
				jobName = args[i];
			} else if (args[i].equalsIgnoreCase("--eval-out-path")) {
				i++;
				evaluationOutPath = args[i];
			} else {
				i++;
				// Unknown parameter, ignore it.
			}
		}

		collectAndStoreStats(googleSpreadSheetName, googleUsername, googlePassword, jobName, nimbusHost, thriftPort,
				evaluationOutPath);

	}

	public static void collectAndStoreStats(String googleSpreadSheetName, String googleUsername, String googlePassword,
			String jobName, String nimbusHost, int thriftPort, String evaluationOutPath) throws TTransportException {
		Map<String, Object> evaluationResults;

		// Create Thrift Client
		TTransport transport = new TFramedTransport(new TSocket(nimbusHost, thriftPort));
		TProtocol protocol = new TBinaryProtocol(transport);
		transport.open();

		Aggregator aggregator = new Aggregator(protocol, jobName);

		if (googleSpreadSheetName != null && googleUsername != null && googlePassword != null) {
			GoogleSpreadsheetHelper spreadsheet = new GoogleSpreadsheetHelper(googleUsername, googlePassword,
					googleSpreadSheetName);
			aggregator.setGoogleSpreadsheet(spreadsheet);
		}

		// This method aggregates all evaluation statistics and writes them into the google spreadsheet.
		evaluationResults = aggregator.aggregateMessagePerHost();

		if (evaluationOutPath != null) { // write sendgraph to filesystem
			File jsonFile;
			String fullJsonFilePath;
			BufferedWriter writer;

			fullJsonFilePath = String.format("%1s/%2s.task.json", evaluationOutPath, jobName);

			try {
				jsonFile = new File(fullJsonFilePath);

				// if file doesnt exists, then create it
				if (!jsonFile.exists()) {
					jsonFile.createNewFile();
				}

				writer = new BufferedWriter(new FileWriter(jsonFile.getAbsoluteFile()));
				writer.write(evaluationResults.get("sankey-tasks-json").toString());
				writer.close();

			} catch (IOException e) {
				System.out.println("Error while writing sendgraph json file to: " + fullJsonFilePath);
				e.printStackTrace();
			}
		}

		aggregator.sendFinishSignal();
	}

	public static String getUsageMessage() {
		StringBuilder builder = new StringBuilder();

		builder.append("Usage: Evaluation").append("\n\n");
		return builder.toString();
	}
}
