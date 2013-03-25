package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import com.google.gdata.client.spreadsheet.SpreadsheetService;
import com.google.gdata.data.spreadsheet.CustomElementCollection;
import com.google.gdata.data.spreadsheet.ListEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetEntry;
import com.google.gdata.data.spreadsheet.SpreadsheetFeed;
import com.google.gdata.data.spreadsheet.WorksheetEntry;
import com.google.gdata.data.spreadsheet.WorksheetFeed;
import com.google.gdata.util.AuthenticationException;
import com.google.gdata.util.ServiceException;

//Installation & Usage as defined here: https://developers.google.com/google-apps/spreadsheets/

/**
 * The class manages the connection to the Google spreadsheet service and provides methods to write data into 
 * the spreadsheet.
 * 
 * @author Thomas Hunziker
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
class GoogleSpreadsheetHelper {

	/**
	 * This is the url that we are going to query when searching for the evaluation spreadsheet.
	 */
	public static final String SPREADSHEET_FEED_URL = "https://spreadsheets.google.com/feeds/spreadsheets/private/full";

	private String spreadsheetName = "";
	private String username = "";
	private String password = "";

	private SpreadsheetService service = null;
	private SpreadsheetEntry spreadsheet = null;
	private WorksheetEntry worksheet = null;

	public GoogleSpreadsheetHelper(String username, String password, String spreadsheetName) {
		this.username = username;
		this.password = password;
		this.spreadsheetName = spreadsheetName;
	}

	/**
	 * Writes the content of a map into the Google spreadsheet associated with this object.
	 * 
	 * @param data
	 *            a set of key value pairs, whose keys have to correspond with the headers of the spreadsheet.
	 * @throws ServiceException
	 * @throws IOException
	 */
	public void addRow(Map<String, Object> data) throws ServiceException, IOException {
		ListEntry rowToAdd = new ListEntry();
		CustomElementCollection keyValuePairs = rowToAdd.getCustomElements();

		// copy all values from the data map into the row
		for (String key : data.keySet()) {
			keyValuePairs.setValueLocal(key, data.get(key).toString());
		}

		// write the data into Google spreadsheet
		URL listFeedUrl = getWorksheet().getListFeedUrl();
		getService().insert(listFeedUrl, rowToAdd);
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

	private SpreadsheetEntry getSpreadSheet() throws IOException, ServiceException {
		if (spreadsheet == null) {
			URL spreadSheetFeedUrl;
			SpreadsheetFeed spreadSheetFeed;
			List<SpreadsheetEntry> spreadsheets;

			spreadSheetFeedUrl = new URL(GoogleSpreadsheetHelper.SPREADSHEET_FEED_URL);
			spreadSheetFeed = getService().getFeed(spreadSheetFeedUrl, SpreadsheetFeed.class);
			spreadsheets = spreadSheetFeed.getEntries(); // get all of them
			
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
			WorksheetFeed worksheetFeed = getService().getFeed(getSpreadSheet().getWorksheetFeedUrl(),
					WorksheetFeed.class);
			List<WorksheetEntry> worksheets = worksheetFeed.getEntries();
			worksheet = worksheets.get(0);
		}
		return worksheet;
	}

}