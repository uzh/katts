package ch.uzh.ddis.katts.evaluation;

import java.util.HashMap;
import java.util.Map;

public class GoogleSpreadsheetTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		GoogleSpreadsheetHelper spreadSheetManager = new GoogleSpreadsheetHelper("katts.evaluation@gmail.com",
				"kattsevaluationongdocs", "katts-evaluation-lorenz");
		Map<String, Object> data = new HashMap<String, Object>();

		data.put("jobname", "test job 1 ");
		data.put("start-date", "test");
		data.put("end-date", "test");
		data.put("duration", "test");
		data.put("total-messages-processed", "test");
		data.put("total-remote-messages", "test");
		data.put("total-local-messages", "test");
		data.put("number-of-nodes", "test");
		data.put("number-of-processors", "test");
		data.put("number-of-processors-per-node", "test");
		data.put("expected-number-of-tasks", "test");
		data.put("number-of-tuples-outputed", "test");
		data.put("factor-of-threads-per-processor", "test");
		data.put("number-of-triples-processed", "test");
		data.put("sankey-tasks-json", "test");
		data.put("sankey-components-json", "test");
		data.put("sankey-hosts-json", "test");

		spreadSheetManager.addRow(data);
		System.out.println("row written");
	}
}
