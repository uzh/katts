package ch.uzh.ddis.katts;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

public class TaskResolver {
	private CSVReader csvReader;
	
	private Map<String, String> tasks = new HashMap<String, String>();
	
	public TaskResolver(String pathToTaskFile) throws IOException {
		csvReader = new CSVReader(new FileReader(pathToTaskFile));
		
		
		List<String[]> lines = csvReader.readAll();
		
		for (String[] line : lines) {
			// The tasks file has the structure: taskId, componentId, hostname
			tasks.put(line[0], line[2]);
		}
	}
	
	public String getHostnameByTask(String taskId) {
		return tasks.get(taskId);
	}
	
}
