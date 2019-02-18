package com.twilio.maxwell.producer;

import com.google.common.util.concurrent.ListenableFuture;

import com.twilio.domain.account.Account;
import com.twilio.maxwell.service.ClientService;
import com.twilio.maxwell.service.TwilioRowMapService;
import com.twilio.piedpiper.client.PiedPiperProducer;
import com.twilio.piedpiper.protocol.DebugEventRecord;
import com.twilio.piedpiper.protocol.exceptions.PayloadParseException;
import com.twilio.piedpiper.protocol.exceptions.PiedPiperProducerMaxAttempt;
import com.twilio.piedpiper.protocol.exceptions.PiedPiperRecordTooLarge;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.twilio.coreutil.Futures.fromGuavaFuture;

public class TwilioProducer extends AbstractProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(TwilioProducer.class);


	private final ClientService clientService;
	private final TwilioRowMapService rowMapService;
	private final PiedPiperProducer piedPiperProducer;

	private final ExecutorService kafkaExecutor = Executors.newCachedThreadPool();

	public TwilioProducer(MaxwellContext context, ClientService clientService, TwilioRowMapService rowMapService, PiedPiperProducer piedPiperProducer) {
		super(context);
		this.clientService = clientService;
		this.rowMapService = rowMapService;
		this.piedPiperProducer = piedPiperProducer;
	}


	@Override
	public void push(RowMap r) throws Exception {

		if (rowMapService.isNotInsert(r)) {
			LOGGER.debug(String.format("Skipping %s - %s", rowMapService.getType(r), rowMapService.toTwilioJson(r)));
			context.setPosition(r);
			return;
		}
		Account account;

		try {
			account = clientService.getAccount(rowMapService.getAccountSid(r));
		} catch (Exception e) {
			LOGGER.error(String.format("Error while getting account info for record %s", r.toJSON()), e);
			// no way to recover probably?
			context.setPosition(r);
			markFailure();
			return;
		}

		DebugEventRecord debugEventRecord;

		try {
			debugEventRecord = rowMapService.buildDebugEventRecord(r, account);
		} catch (Exception e) {
			LOGGER.error(String.format("Error while building debug payload from %s", r.toJSON()), e);
			// no way to recover probably?
			context.setPosition(r);
			markFailure();
			return;
		}

		push(debugEventRecord,r);
	}


	private void push(DebugEventRecord record, RowMap r) {
		final ListenableFuture<RecordMetadata> lf;
		String sid = record.getSid().toString();
		String accountSid = record.getAccountSid().toString();
		try {
			lf = piedPiperProducer.send(record);
		} catch (final PiedPiperRecordTooLarge e) {
			LOGGER.warn("[{}] [{}] Unexpected payload is too large exception from a db, record size is {}", sid, accountSid, e.getSize());
			markFailure();
			return;
		} catch (final PayloadParseException e) {
			LOGGER.warn("[{}] [{}] Failed to parse db row into DebugEventRecord", sid, accountSid);
			markFailure();
			return;
		} catch (final PiedPiperProducerMaxAttempt e) {
			LOGGER.warn("[{}] [{}] Maxed out retry attempts", sid, accountSid);
			markFailure();
			return;
		} catch (Exception e) {
			LOGGER.warn("[{}] [{}] Exception while sending debug event: {}", sid, accountSid, e);
			markFailure();
			return;
		}

		fromGuavaFuture(lf)
				.thenAcceptAsync(recordMetadata -> {

					context.setPosition(r);
					LOGGER.info("[{}] [{}] successfully sent debug event to piedPiper {}", sid, accountSid, record);
					markSuccess();

				}, kafkaExecutor)
				.exceptionally(e -> {

					LOGGER.error("[{}] [{}] failed to send debug event to piedPiper: {}", sid, accountSid, e);
					markFailure();
					return null;
				});
	}

	private void markSuccess() {
		succeededMessageCount.inc();
		succeededMessageMeter.mark();
	}

	private void markFailure() {
		failedMessageCount.inc();
		failedMessageMeter.mark();
	}

}
