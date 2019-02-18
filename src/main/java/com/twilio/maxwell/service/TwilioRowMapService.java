package com.twilio.maxwell.service;

import com.twilio.domain.account.Account;
import com.twilio.piedpiper.protocol.DebugEventRecord;
import com.zendesk.maxwell.row.RowMap;

public interface TwilioRowMapService {
	boolean isInsert(RowMap rowMap);
	default boolean isNotInsert(RowMap rowMap) {
		return ! isInsert(rowMap);
	}

	String getAccountSid(RowMap rowMap);
	String getSid(RowMap rowMap);

	String toTwilioJson(RowMap rowMap);

	default String getType(RowMap rowMap) {
		return rowMap.getRowType();
	}

	DebugEventRecord buildDebugEventRecord(RowMap rowMap, Account account) throws ParseException;


	final class ParseException extends Exception {
		public ParseException() {
		}

		public ParseException(String message) {
			super(message);
		}

		public ParseException(String message, Throwable cause) {
			super(message, cause);
		}

		public ParseException(Throwable cause) {
			super(cause);
		}

		public ParseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
			super(message, cause, enableSuppression, writableStackTrace);
		}
	}
}
