package ch.uzh.ddis.katts.bolts.source;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileTripleReaderTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConverterLong() {
		FileTripleReader reader = new FileTripleReader();
		assertEquals(Long.valueOf(1L), reader.convertStringToObject("1"));
	}
		
	@Test
	public void testConverterDouble() {
		FileTripleReader reader = new FileTripleReader();
		assertEquals(Double.valueOf(1.0D), reader.convertStringToObject("1.0D"));
	}

	@Test
	public void testConverterDateTime() {
		FileTripleReader reader = new FileTripleReader();
		Calendar cal = GregorianCalendar.getInstance();
		cal.set(2001, 2, 4, 5, 6, 7);
		cal.set(Calendar.MILLISECOND, 0);
		
		long calDate = cal.getTime().getTime();
		long parseDate = ((Date)reader.convertStringToObject("2001-03-04T05:06:07")).getTime();
		assertEquals(calDate, parseDate);
		
	}
	
}
