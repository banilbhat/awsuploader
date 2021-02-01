package com.openthoughts.ingest.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;
public class AppLockTest {

  @Test
  public void appLock() {
    final boolean appLocked = AppLock.setLock("INGEST_CLIENT_LOCK_KEY");
    assertThat(appLocked, is(true));
    AppLock.releaseLock();
  }
}
