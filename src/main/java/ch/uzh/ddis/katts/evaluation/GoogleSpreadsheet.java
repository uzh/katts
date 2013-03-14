package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CellEntry;
import com.google.gdata.data.spreadsheet.CellFeed;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;

//Installation & Usage as defined here: https://developers.google.com/google-apps/spreadsheets/

/**
 * This class pushes the data to the google spreadsheet.
 * 
 * @author Thomas Hunziker
 *
 */
class GoogleSpreadsheet {

	private String spreadsheetName = "";
	private String username = "";
	private String password = "";

	private SpreadsheetService service = null;
	private SpreadsheetEntry spreadsheet = null;
	private WorksheetEntry worksheet = null;
	private Map<String, Integer> columnMapping;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
	private SecureRandom random = new SecureRandom();

	public GoogleSpreadsheet(String username, String password, String spreadsheetName) {
		this.username = username;
		this.password = password;
		this.spreadsheetName = spreadsheetName;
	}

	public void addRow(JobData jobData) throws IOException, ServiceException, URISyntaxException {

		
		// Since we cant prevent simultanous writing, we need to to the workaround to write to the
		// field an check after a few seconds, if our data row has been written. 
		int rowId =  reserveRow(jobData.getJobName());
		
		this.updateCellAt(rowId, "start_date", dateFormat.format(new Date(jobData.getJobStart())));
		this.updateCellAt(rowId, "end_date", dateFormat.format(new Date(jobData.getJobEnd())));
		this.updateCellAt(rowId, "duration", jobData.getJobDuration() / 1000);
		this.updateCellAt(rowId, "total_messages_processed", jobData.getTotalMessages());
		this.updateCellAt(rowId, "total_remote_messages", jobData.getTotalRemoteMessages());
		this.updateCellAt(rowId, "total_local_messages", jobData.getTotalLocalMessages());
		this.updateCellAt(rowId, "number_of_nodes", jobData.getNumberOfNodes());
		this.updateCellAt(rowId, "number_of_processors", jobData.getNumberOfProcessors());
		this.updateCellAt(rowId, "number_of_processors_per_node", jobData.getNumberOfProcessorsPerNode());
		this.updateCellAt(rowId, "expected_number_of_tasks", jobData.getExpectedNumberOfTasks());
		this.updateCellAt(rowId, "number_of_tuples_outputed", jobData.getNumberOfTuplesOutputed());
		this.updateCellAt(rowId, "factor_of_threads_per_processor", jobData.getFactorOfThreadsPerProcessor());
		this.updateCellAt(rowId, "number_of_triples_processed", jobData.getNumberOfTriplesProcessed());
		this.updateCellAt(rowId, "sankey_tasks_json", jobData.getSankeyDataTasks());
		this.updateCellAt(rowId, "sankey_components_json", jobData.getSankeyDataComponents());
		this.updateCellAt(rowId, "sankey_hosts_json", jobData.getSankeyDataHosts());

	}
	
	private int reserveRow(String jobName) throws IOException, ServiceException, URISyntaxException {
		int rowId = getNextEmptyRowNumber();
		this.updateCellAt(rowId, "job_name", jobName);
		
		// Wait at least 3 seconds, before check if our row name was written
		try {
			Thread.sleep(1000 * random.nextInt(5) + 3000);
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not wait for checking if the row is reserved or not, because the thread was interrupted.", e);
		}
		
		String jobNameRemote = getCellAt(rowId, "job_name").getCell().getInputValue();

		if (jobNameRemote.equals(jobName)) {
			return rowId;
		}
		else {
			// Try to get another row.
			return reserveRow(jobName);
		}
		
	}
	
	
	public void updateCellAt(int rowId, String columnName, long value) throws IOException, ServiceException, URISyntaxException {
		updateCellAt(rowId, columnName, Long.toString(value));
	}
	
	public void updateCellAt(int rowId, String columnName, float value) throws IOException, ServiceException, URISyntaxException {
		updateCellAt(rowId, columnName, Float.toString(value));
	}
	
	public void updateCellAt(int rowId, String columnName, double value) throws IOException, ServiceException, URISyntaxException {
		updateCellAt(rowId, columnName, Double.toString(value));
	}
	
	

	public void updateCellAt(int rowId, String columnName, String value) throws IOException, ServiceException,
			URISyntaxException {
		Integer columnId = this.getColumnNumberByName(columnName);
		
		// Skip the value, if it was not found in the spreadsheet
		if (columnId == null) {
			return;
		}
		
		if (this.getWorksheet().getRowCount() < rowId) {
			throw new RuntimeException("Not enough rows. You need to add some rows.");
		}
		
		if (this.getWorksheet().getColCount() < columnId) {
			throw new RuntimeException("Not enough columns. You need to add some column.");
		}
		
		CellEntry cell = new CellEntry(rowId, columnId, value);
		this.getService().insert(this.getWorksheet().getCellFeedUrl(), cell);
	}

	public CellEntry getCellAt(int rowId, String columnName) throws MalformedURLException, AuthenticationException,
			URISyntaxException, IOException, ServiceException {

		int columnId = this.getColumnNumberByName(columnName);

		return getCellAt(rowId, columnId);
	}

	public CellEntry getCellAt(int rowId, int columnId) throws MalformedURLException, AuthenticationException,
			URISyntaxException, IOException, ServiceException {

		URL cellFeedUrl = new URI(this.getWorksheet().getCellFeedUrl().toString() + "?min-row=" + rowId + "&max-row="
				+ (rowId + 1) + "&min-col=" + columnId + "&max-col=" + (columnId + 1)).toURL();
		
		
		CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);

		List<CellEntry> entries = cellFeed.getEntries();
		if (entries.size() == 0) {
			throw new RuntimeException("The given Cell was not found. (at row: " + rowId + "   column: " + columnId
					+ ")");
		}

		return entries.get(0);
	}

	public String getSpreadsheetName() {
		return spreadsheetName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public int getNextEmptyRowNumber() throws MalformedURLException, AuthenticationException, URISyntaxException, IOException, ServiceException {
		
		int jobNameColumnId = getColumnNumberByName("job_name");
		
		URL cellFeedUrl = new URI(this.getWorksheet().getCellFeedUrl().toString() + "?min-row=1&min-col=" + jobNameColumnId + "&max-col=" + jobNameColumnId).toURL();
		
		CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);

		List<CellEntry> entries = cellFeed.getEntries();
		
		return entries.get(entries.size()-1).getCell().getRow() + 1;
	}

	public Integer getColumnNumberByName(String columnName) throws MalformedURLException, AuthenticationException,
			URISyntaxException, IOException, ServiceException {

		if (columnName == null) {
			throw new RuntimeException("You must specify a column name. Null given.");
		}

		if (columnMapping == null) {
			columnMapping = new HashMap<String, Integer>();

			URL cellFeedUrl = new URI(this.getWorksheet().getCellFeedUrl().toString()
					+ "?min-row=1&min-col=1&max-row=1").toURL();
			CellFeed cellFeed = service.getFeed(cellFeedUrl, CellFeed.class);

			// Iterate through each cell, printing its value.
			for (CellEntry cell : cellFeed.getEntries()) {
				columnMapping.put(cell.getCell().getValue(), cell.getCell().getCol());
			}
		}
		
		return columnMapping.get(columnName);
	}

	private SpreadsheetEntry getSpreadSheet() throws IOException, ServiceException {
		if (spreadsheet == null) {
			// Define the URL to request. This should never change.
			URL SPREADSHEET_FEED_URL = new URL("https://spreadsheets.google.com/feeds/spreadsheets/private/full");

			// Make a request to the API and get all spreadsheets.
			SpreadsheetFeed feed = this.getService().getFeed(SPREADSHEET_FEED_URL, SpreadsheetFeed.class);
			List<SpreadsheetEntry> spreadsheets = feed.getEntries();

			if (spreadsheets.size() == 0) {
				throw new RuntimeException("No spreadsheet defined in this google account.");
			}

			for (SpreadsheetEntry s : spreadsheets) {
				if (s.getTitle().getPlainText().equalsIgnoreCase(getSpreadsheetName())) {
					spreadsheet = s;
					return s;
				}
			}

			throw new RuntimeException("No spreadsheet found with name '" + getSpreadsheetName() + "'.");
		} else {
			return spreadsheet;
		}
	}

	private SpreadsheetService getService() throws AuthenticationException {

		if (this.service == null) {
			service = new SpreadsheetService("KATTS-Evaluation-v1");
			service.setUserCredentials(getUsername(), getPassword());
		}

		return service;
	}

	private WorksheetEntry getWorksheet() throws AuthenticationException, IOException, ServiceException {
		if (worksheet == null) {
			WorksheetFeed worksheetFeed = this.getService().getFeed(this.getSpreadSheet().getWorksheetFeedUrl(),
					WorksheetFeed.class);
			List<WorksheetEntry> worksheets = worksheetFeed.getEntries();
			worksheet = worksheets.get(0);
		}
		return worksheet;
	}

}
