package com.twilio.maxwell.service;

import com.google.common.base.Throwables;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.twilio.domain.account.Account;
import com.twilio.piedpiper.protocol.DebugEventRecord;
import com.twilio.piedpiper.protocol.LogLevel;
import com.twilio.piedpiper.protocol.PayloadType;
import com.twilio.piedpiper.protocol.Util;
import com.twilio.piedpiper.protocol.payloads.WebhookPayload;
import com.twilio.piedpiper.protocol.webhooks.ClassicRequest;
import com.twilio.piedpiper.protocol.webhooks.ClassicResponse;
import com.twilio.sids.AccountSid;
import com.twilio.sids.NotificationSid;
import com.twilio.sids.NotificationSummarySid;
import com.twilio.sids.Sid;
import com.twilio.sids.SidUtil;
import com.twilio.sids.UnknownSidPrefixException;
import com.zendesk.maxwell.row.RowMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class TwilioRowMapServiceImpl implements TwilioRowMapService {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwilioRowMapServiceImpl.class);

	private Logger getLogger() {
		return LOGGER;
	}

	@Override
	public boolean isInsert(RowMap rowMap) {
		return "insert".equalsIgnoreCase(rowMap.getRowType());
	}

	@Override
	public String getAccountSid(RowMap rowMap) {
		return getStringValue(rowMap,"accountsid");
	}

	@Override
	public String getSid(RowMap rowMap) {
		return getStringValue(rowMap,"sid");
	}


	@Override
	public String toTwilioJson(RowMap rowMap) {
		Map<String, Object> data = new LinkedHashMap<>(rowMap.getData()); // create a copy, don't touch input param
		try {
			// If we don't have an epoch, let's try to set one
			// Since we're only worried about MySQL we can use a naive date format
			if (!data.get("datecreated").toString().matches("\\d+")) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss");
				Date date = sdf.parse(data.get("datecreated").toString());
				long millis = date.getTime();
				data.put("datecreated", millis / 1000);
			}

			if (data.get("@id") == null && data.get("sid") != null) {
				data.put("@id", data.get("sid"));
			}
		} catch (Exception ex) {
			LOGGER.error("Failed to set datecreated", ex);
		}
		try {
			return new ObjectMapper().writeValueAsString(data);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}



	@Override
	public DebugEventRecord buildDebugEventRecord(RowMap rowMap, Account account) throws ParseException {
		LogLevel logLevel = getLogLevel(rowMap);

		Sid correlationSid = null;
		try {
			correlationSid = parseCorrelationSid(rowMap).orElse(null);
		} catch (UnknownSidPrefixException e) {
			LOGGER.warn("[{}] [{}] unknown callsid {}; setting correlationSid as null", getSid(rowMap), getAccountSid(rowMap), getStringValue(rowMap,"callsid"));
		}

		Sid serviceSid = null;
		try {
			serviceSid = parseServiceSid(rowMap).orElse(null);
		} catch (UnknownSidPrefixException e) {
			LOGGER.warn("[{}] [{}] unknown servicesid {}; setting serviceSid as null", getSid(rowMap), getAccountSid(rowMap), getStringValue(rowMap,"servicesid"));
		}


		Optional<AccountSid> parent = AccountHelper.parentAccountSid(account);

		if (parent.isPresent()) {
			LOGGER.info(String.format("Found parent account [%s] from child [%s]", parent.get().getValue(), account.getSid().getValue()));
		} else {
			LOGGER.debug(String.format("%s is the parent account", account.getSid().getValue()));
		}

		return DebugEventRecord.builder()
				.withSid(new NotificationSid(getSid(rowMap)))
				.withAccountSid(account.getSid())
				.withDateCreated(Util.parseDate(getStringValue(rowMap,"datecreated")))
				.withCorrelationSid(correlationSid)
				.withLogLevel(logLevel)
				.withPayloadType(PayloadType.WEBHOOK)
				.withPayload(getWebhookPayload(rowMap))
				.withParentAccountSid(parent.orElse(null))
				.withRequestSid(null)
				.withServiceSid(serviceSid)
				.withProductName(getProductName(correlationSid))
				.withPublisherMeta(null)
				.withMeta(null)
				.build();
	}



	private String _getLogLevel(RowMap rowMap) {
		return getStringValue(rowMap, "log");
	}

	private LogLevel getLogLevel(RowMap rowMap) throws ParseException {
		String logLevel = _getLogLevel(rowMap);
		if (logLevel == null) {
			return LogLevel.DEBUG;
		}
		try {
			return LogLevel.getLogLevel(logLevel);
		} catch (Exception e) {
			throw new ParseException(parseErrorMessage("log",logLevel), e);
		}
	}


	private Optional<Sid> parseSid(RowMap rowMap, String name) throws UnknownSidPrefixException {
		String sid = getStringValue(rowMap,name);
		if (sid == null) return Optional.ofNullable(null);
		return Optional.of(SidUtil.fromHex(sid));
	}


	private Optional<Sid> parseCorrelationSid(RowMap rowMap) throws UnknownSidPrefixException {
		return parseSid(rowMap,"callsid");
	}

	private Optional<Sid> parseServiceSid(RowMap rowMap) throws UnknownSidPrefixException {
		return parseSid(rowMap,"servicesid");
	}

	private String getStringValue(RowMap rowMap, String key) {
		Object value = rowMap.getData(key);
		return value != null ? String.valueOf(value) : null;
	}


	private WebhookPayload getWebhookPayload(final RowMap r) {
		final Object dateUpdated = r.getData("dateupdated");
		final Object log = r.getData("log");
		final Object messageText = r.getData("messagetext");
		final Object errorCode = r.getData("errorcode");
		final Object responseBody = r.getData("responsebody");
		final Object requestMethod = r.getData("requestmethod");
		final Object requestUrl = r.getData("requesturl");
		final Object requestVariables = r.getData("requestvariables");
		final Object responseHeaders = r.getData("responseheaders");
		final Object flags = r.getData("flags");
		final Object apiVersion = r.getData("apiversion");
		final Object summarySid = r.getData("notificationsummarysid");
		final Object requestHeader = r.getData("requestheader");
		final Object messageDate = r.getData("messagedate");

		Instant messageDateInstant;
		if (messageDate != null && messageDate.toString() != null && !messageDate.toString().isEmpty()) {
			messageDateInstant = Util.parseDate(messageDate.toString());
		} else {
			Object dateCreated = r.getData("datecreated");
			if (dateCreated == null) throw new RuntimeException("datecreated slould be set");
			messageDateInstant = Util.parseDate(dateCreated.toString());
		}

		Integer flagsInt = null;
		if (flags != null) {
			try {
				flagsInt = (int) (long) flags;
			} catch (Exception e) {
				getLogger().warn("[{}] [{}] failed to parse flag {}: {}", getSid(r), getAccountSid(r), flags, e);
			}
		}

		Integer errorCodeInt = null;
		if (errorCode != null) {
			try {
				errorCodeInt = (int) (long) errorCode;
			} catch (Exception e) {
				getLogger().warn("[{}] [{}] failed to parse errorCode {}: {}", getSid(r), getAccountSid(r), errorCode, e);
			}
		}

		Integer logInt = null;
		if (log != null) {
			try {
				logInt = (int) (long) log;
			} catch (Exception e) {
				getLogger().warn("[{}] [{}] failed to parse log {}: {}", getSid(r), getAccountSid(r), log, e);
			}
		}

		final ClassicRequest request = ClassicRequest.builder()
				.withHeaders(requestHeader == null ? null : Util.queryParamsToMap(requestHeader.toString()))
				.withParameters(requestVariables == null ? null : Util.queryParamsToMap(requestVariables.toString()))
				.withMethod((String) requestMethod)
				.withUrl((String) requestUrl)
				.build();

		final ClassicResponse response = ClassicResponse.builder()
				.withHeaders(responseHeaders == null ? null : Util.queryParamsToMap(responseHeaders.toString()))
				.withBody((String) responseBody)
				.withStatusCode(null)
				.build();

		return WebhookPayload.builder()
				.withRequest(request)
				.withResponse(response)
				.withApiVersion((String) apiVersion)
				.withFlags(flagsInt)
				.withMessageText(Util.queryParamsToMap((String) messageText))
				.withErrorCode(errorCodeInt)
				.withLog(logInt)
				.withNotificationSummarySid(summarySid == null ? null : new NotificationSummarySid((String) summarySid))
				.withMessageDate(messageDateInstant)
				.withDateUpdated(Util.parseDate((String) dateUpdated))
				.build();
	}

	private String getProductName(Sid sid) {
		if (sid == null) {
			return null;
		}

		switch (sid.toString().substring(0, 2).toUpperCase()) {
			case "SM":
				return "Programmable SMS";
			case "CA":
				return "Programmable Voice";
			case "MM":
				return "Programmable SMS";
			case "RU":
				return "API";
			case "RQ":
				return "API";
			case "XR":
				return "Add-ons";
			case "NO":
				return "Notification";
			case "PV":
				return "Voice Service";
		}

		return null;
	}




	private static String parseErrorMessage(String propertyName, Object propertyValue) {
		return "Cannot parse " + propertyName + " from value " + propertyValue;
	}

}
