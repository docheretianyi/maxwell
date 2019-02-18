package com.twilio.maxwell.service;

import com.twilio.domain.account.Account;
import com.twilio.sids.AccountSid;

import java.util.Optional;

public interface AccountHelper {

	Long SKIP_DEBUG_EVENTS_FLAG_ID = 508L;

	static boolean skipDebugEventForAccount(Account account) {
		return account.hasAccountFlag(SKIP_DEBUG_EVENTS_FLAG_ID);
	}

	static Optional<AccountSid> parentAccountSid(Account account) {
		return Optional.ofNullable(
				account.getParentAccountSid().isPresent() ?
						account.getParentAccountSid().get() : null
		);
	}
}
