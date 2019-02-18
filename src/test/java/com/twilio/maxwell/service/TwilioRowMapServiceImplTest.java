package com.twilio.maxwell.service;

import com.google.common.base.Optional;

import com.twilio.domain.account.Account;
import com.twilio.piedpiper.protocol.DebugEventRecord;
import com.twilio.sids.AccountSid;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.mockito.Mockito.*;

public class TwilioRowMapServiceImplTest {
	@Mock
	Logger LOGGER;
	@InjectMocks
	TwilioRowMapServiceImpl twilioRowMapServiceImpl;

	private ObjectMapper objectMapper = new ObjectMapper();


	@Before
	public void setUp() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testIsInsert() throws Exception {
		boolean result = twilioRowMapServiceImpl.isInsert(new RowMap("insert", null, null, 1000l, null, null, null, null));
		Assert.assertTrue(result);
		result = twilioRowMapServiceImpl.isInsert(new RowMap("INSERT", null, null, 1000l, null, null, null, null));
		Assert.assertTrue(result);

		result = twilioRowMapServiceImpl.isInsert(new RowMap("UPDATE", null, null, 1000l, null, null, null, null));
		Assert.assertFalse(result);

		result = twilioRowMapServiceImpl.isInsert(new RowMap("delete", null, null, 1000l, null, null, null, null));
		Assert.assertFalse(result);

		result = twilioRowMapServiceImpl.isInsert(new RowMap(null, null, null, 1000l, null, null, null, null));
		Assert.assertFalse(result);
	}

	@Test
	public void testGetAccountSid() throws Exception {
		RowMap rw = new RowMap("type", "database", "table", Long.valueOf(1), Arrays.<String>asList("String"), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), "rowQuery");

		rw.getData().put("accountsid","accountsidValue");

		String result = twilioRowMapServiceImpl.getAccountSid(rw);
		Assert.assertEquals("accountsidValue", result);

		rw.getData().put("accountsid","someOtherValue");

		result = twilioRowMapServiceImpl.getAccountSid(rw);
		Assert.assertEquals("someOtherValue", result);
	}

	@Test
	public void testGetSid() throws Exception {
		RowMap rw = new RowMap("type", "database", "table", Long.valueOf(1), Arrays.<String>asList("String"), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), "rowQuery");

		rw.getData().put("sid","sidValue");

		String result = twilioRowMapServiceImpl.getSid(rw);
		Assert.assertEquals("sidValue", result);

		rw.getData().put("sid","someOtherValue");

		result = twilioRowMapServiceImpl.getSid(rw);
		Assert.assertEquals("someOtherValue", result);
	}

	@Test
	public void testToTwilioJson() throws Exception {
		RowMap rw = new RowMap("type", "database", "table", Long.valueOf(1), Arrays.<String>asList("String"), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), "rowQuery");

		rw.getData().put("datecreated", "2019-02-14 11:25:15");
		rw.getData().put("sid", "12345");
		rw.getData().put("k1","val1");
		rw.getData().put("k2","val2");

		String result = twilioRowMapServiceImpl.toTwilioJson(rw);
		String dc = String.valueOf(date("2019-02-14 11:25:15"));
		String expected = "{\"datecreated\":" + dc + ",\"sid\":\"12345\",\"k1\":\"val1\",\"k2\":\"val2\",\"@id\":\"12345\"}";
		Assert.assertEquals(toComparableMap(expected), toComparableMap(result));
	}

	private long date(String val) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
		Date date = sdf.parse(val);
		long millis = date.getTime();
		return millis / 1000;
	}

	@Test
	public void testBuildDebugEventRecord() throws Exception {

		Account account = mock(Account.class);
		when(account.getParentAccountSid()).thenReturn(Optional.absent());
		when(account.getSid()).thenReturn(AccountSid.parse("AC12345678901234567890123456789012"));

		RowMap rw = new RowMap("type", "database", "table", Long.valueOf(1), Arrays.<String>asList("String"), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), new Position(new BinlogPosition("gtidSetStr", "gtid", 0L, "file"), 0L), "rowQuery");

		rw.getData().put("datecreated", "2019-02-14 11:25:15");
		rw.getData().put("sid", "NO12345678901234567890123456789012");
		rw.getData().put("k1","val1");
		rw.getData().put("k2","val2");

		DebugEventRecord result = twilioRowMapServiceImpl.buildDebugEventRecord(rw,account);


		Assert.assertEquals(toComparableMap(debugEventRecord1), toComparableMap(result));


		rw.getData().put("callsid", "NO12345678901234567890123456789555");
		rw.getData().put("servicesid", "NO12345678901234567890123456789777");
		rw.getData().put("log","WARNING");

		when(account.getParentAccountSid()).thenReturn(Optional.of(AccountSid.parse("AC12345678901234567890123456781111")));


		result = twilioRowMapServiceImpl.buildDebugEventRecord(rw,account);

		Assert.assertEquals(toComparableMap(debugEventRecord2), toComparableMap(result));


	}



	@Test
	public void testGetType() throws Exception {
		String result = twilioRowMapServiceImpl.getType(new RowMap("type", null, null, 1000l, null, null, null, null));
		Assert.assertEquals("type", result);

		result = twilioRowMapServiceImpl.getType(new RowMap("INSERT", null, null, 1000l, null, null, null, null));
		Assert.assertEquals("INSERT", result);
	}


	private Map toComparableMap(DebugEventRecord object) {
		try {
			return objectMapper.readValue(object.toString(), Map.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Map toComparableMap(String record) {
		try {
			return objectMapper.readValue(record, Map.class);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final String debugEventRecord1 = "{\n" +
			"  \"sid\": \"NO12345678901234567890123456789012\",\n" +
			"  \"account_sid\": \"AC12345678901234567890123456789012\",\n" +
			"  \"master_account_sid\": null,\n" +
			"  \"request_sid\": null,\n" +
			"  \"service_sid\": null,\n" +
			"  \"correlation_sid\": null,\n" +
			"  \"date_created\": \"2019-02-14T19:25:15Z\",\n" +
			"  \"log_level\": \"DEBUG\",\n" +
			"  \"payload_type\": \"webhook\",\n" +
			"  \"payload\": \"{\\\"date_updated\\\":null,\\\"log\\\":null,\\\"flags\\\":null,\\\"response_body\\\":null,\\\"request_method\\\":null,\\\"api_version\\\":null,\\\"notification_summary_sid\\\":null,\\\"request_url\\\":null,\\\"message_text\\\":\\\"\\\",\\\"response_headers\\\":\\\"\\\",\\\"request_header\\\":\\\"\\\",\\\"message_date\\\":\\\"2019-02-14T19:25:15Z\\\",\\\"request_variables\\\":\\\"\\\",\\\"error_code\\\":null,\\\"response_status_code\\\":null}\",\n" +
			"  \"product_name\": null,\n" +
			"  \"publisher\": null,\n" +
			"  \"publisher_meta\": null,\n" +
			"  \"meta\": null\n" +
			"}";


	private static final String debugEventRecord2 = "{\n" +
			"  \"sid\": \"NO12345678901234567890123456789012\",\n" +
			"  \"account_sid\": \"AC12345678901234567890123456789012\",\n" +
			"  \"master_account_sid\": \"AC12345678901234567890123456781111\",\n" +
			"  \"request_sid\": null,\n" +
			"  \"service_sid\": \"NO12345678901234567890123456789777\",\n" +
			"  \"correlation_sid\": \"NO12345678901234567890123456789555\",\n" +
			"  \"date_created\": \"2019-02-14T19:25:15Z\",\n" +
			"  \"log_level\": \"WARNING\",\n" +
			"  \"payload_type\": \"webhook\",\n" +
			"  \"payload\": \"{\\\"date_updated\\\":null,\\\"log\\\":null,\\\"flags\\\":null,\\\"response_body\\\":null,\\\"request_method\\\":null,\\\"api_version\\\":null,\\\"notification_summary_sid\\\":null,\\\"request_url\\\":null,\\\"message_text\\\":\\\"\\\",\\\"response_headers\\\":\\\"\\\",\\\"request_header\\\":\\\"\\\",\\\"message_date\\\":\\\"2019-02-14T19:25:15Z\\\",\\\"request_variables\\\":\\\"\\\",\\\"error_code\\\":null,\\\"response_status_code\\\":null}\",\n" +
			"  \"product_name\": \"Notification\",\n" +
			"  \"publisher\": null,\n" +
			"  \"publisher_meta\": null,\n" +
			"  \"meta\": null\n" +
			"}";
}
