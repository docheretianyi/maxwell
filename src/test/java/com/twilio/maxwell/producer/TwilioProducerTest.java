package com.twilio.maxwell.producer;

import com.google.api.core.ListenableFutureToApiFuture;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import com.codahale.metrics.Counter;
import com.twilio.domain.account.Account;
import com.twilio.maxwell.service.ClientService;
import com.twilio.maxwell.service.TwilioRowMapService;
import com.twilio.maxwell.service.TwilioRowMapServiceImpl;
import com.twilio.piedpiper.client.PiedPiperProducer;
import com.twilio.sids.AccountSid;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellTestJSON;
import com.zendesk.maxwell.MaxwellTestSupport;
import com.zendesk.maxwell.MaxwellTestWithIsolatedServer;
import com.zendesk.maxwell.MysqlIsolatedServer;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.row.RowMap;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.hamcrest.CoreMatchers.*;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class TwilioProducerTest extends MaxwellTestWithIsolatedServer {

	@Test
	public void testTwilioProducer() throws Exception{

		server.execute("CREATE TABLE `twilioevent` (\n" +
				"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
				"  `accountsid` varchar(96),\n" +
				"  `sid` varchar(96),\n" +
				"  `datecreated`  varchar(96),\n" +
				"  PRIMARY KEY (id)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8\n");



		MaxwellOutputConfig outputConfig = new MaxwellOutputConfig();
		outputConfig.encryptionMode = EncryptionMode.ENCRYPT_DATA;
		outputConfig.secretKey = "aaaaaaaaaaaaaaaa";
		List<RowMap> list;
		String input[] = {"insert into twilioevent set " +
				"accountsid = 'AC12345678901234567890123456789012', " +
				"sid = 'NO12345678901234567890123456789012'," +
				"datecreated = '2019-02-14 11:25:15'"};
		list = getRowsForSQL(input);


		MysqlIsolatedServer lowerCaseServer = new MysqlIsolatedServer();
		lowerCaseServer.boot("--lower-case-table-names=1");
		MaxwellContext context = MaxwellTestSupport.buildContext(lowerCaseServer.getPort(), null, null);


		ClientService clientService = mock(ClientService.class);
		TwilioRowMapService rowMapService = new TwilioRowMapServiceImpl();

		PiedPiperProducer piedPiperProducer = mock(PiedPiperProducer.class);

		RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic",1), 1L,1L,1L,1L,1,1);

		when(piedPiperProducer.send(any())).thenAnswer(correctAnswer(recordMetadata));

		Account account = mock(Account.class);
		when(account.getParentAccountSid()).thenReturn(Optional.absent());
		when(account.getSid()).thenReturn(AccountSid.parse("AC12345678901234567890123456789012"));
		when(clientService.getAccount("AC12345678901234567890123456789012")).thenReturn(account);

		TwilioProducer twilioProducer = new TwilioProducer(context,clientService,rowMapService,piedPiperProducer);

		Field sf = twilioProducer.getClass().getSuperclass().getDeclaredField("succeededMessageCount");
		Field ff = twilioProducer.getClass().getSuperclass().getDeclaredField("failedMessageCount");

		sf.setAccessible(true);
		ff.setAccessible(true);

		Counter failedMessageCount = (Counter) ff.get(twilioProducer);
		Counter succeededMessageCount = (Counter) sf.get(twilioProducer);

		twilioProducer.push(list.get(0));


		verify(piedPiperProducer, times(1)).send(any());

		assertThat(failedMessageCount.getCount(), is(0L));
		assertThat(succeededMessageCount.getCount(), is(1L));


		when(clientService.getAccount(any())).thenReturn(null);


		twilioProducer.push(list.get(0));


		assertThat(failedMessageCount.getCount(), is(1L));
		assertThat(succeededMessageCount.getCount(), is(1L));


		verify(piedPiperProducer, times(1)).send(any());


		when(clientService.getAccount("AC12345678901234567890123456789012")).thenReturn(account);
		when(piedPiperProducer.send(any())).thenThrow(new RuntimeException());

		twilioProducer.push(list.get(0));

		assertThat(failedMessageCount.getCount(), is(2L));
		assertThat(succeededMessageCount.getCount(), is(1L));

	}


	private static Answer<ListenableFuture<RecordMetadata>> correctAnswer(RecordMetadata recordMetadata) {
		return invocationOnMock ->
				new ListenableFuture<RecordMetadata>() {
					@Override
					public void addListener(Runnable runnable, Executor executor) {
						executor.execute(runnable);
					}

					@Override
					public boolean cancel(boolean mayInterruptIfRunning) {
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}

					@Override
					public boolean isDone() {
						return true;
					}

					@Override
					public RecordMetadata get() throws InterruptedException, ExecutionException {
						return recordMetadata;
					}

					@Override
					public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
						return null;
					}
				};

	}



}
