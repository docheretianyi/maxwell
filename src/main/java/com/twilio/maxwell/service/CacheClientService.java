package com.twilio.maxwell.service;

import static com.google.common.base.Throwables.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.twilio.client.account.AccountClient;
import com.twilio.domain.account.Account;
import com.twilio.sids.AccountSid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class CacheClientService implements ClientService {

	private static final Logger LOGGER = LoggerFactory.getLogger(CacheClientService.class);

	private final AccountClient accountClient;

	private final LoadingCache<String,Account> cache = CacheBuilder.newBuilder()
			.build(new CacheLoader<String, Account>() {
				@Override
				public Account load(String id) throws Exception {
					return getAccountNoCache(id);
				}
			});

	public CacheClientService(AccountClient accountClient) {
		this.accountClient = accountClient;
	}

	@Override
	public Account getAccount(String id) {
		try {
			return cache.get(id);
		} catch (ExecutionException e) {
			LOGGER.error("Error while getting account data",e);
			throw propagate(e);
		}
	}

	private Account getAccountNoCache(String id) {
		final AccountSid acSid = AccountSid.parse(id);
		try {
			return accountClient.get(acSid).get();
		} catch (InterruptedException|ExecutionException e) {
			throw propagate(e);
		}
	}
}
