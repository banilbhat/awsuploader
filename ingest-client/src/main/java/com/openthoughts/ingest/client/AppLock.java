package com.openthoughts.ingest.client;


import com.openthoughts.ingest.client.exceptions.IngestRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.MessageDigest;

/**
 * The Class AppLock.
 *
 * @author rumatoest
 * @url http://nerdydevel.blogspot.com/2012/07/run-only-single-java-application-instance.html
 */
public class AppLock {

  private AppLock() {
  }

  private File lockFile = null;
  private FileLock lock = null;
  private FileChannel lockChannel = null;
  private FileOutputStream lockStream = null;


  /**
   * Instantiates a new app lock.
   *
   * @param key Unique application key
   * @throws Exception The exception
   */
  private AppLock(final String key) throws Exception {

    final String tmp_dir = System.getProperty("java.io.tmpdir")
        .endsWith(System.getProperty("file.separator")) ? System.getProperty("java.io.tmpdir") : System.getProperty("java.io.tmpdir") + System
        .getProperty("file.separator");

    // Acquire MD5
    try {
      final MessageDigest md = MessageDigest
          .getInstance("MD5");
      md.reset();
      String hash_text = new BigInteger(1, md.digest(key
          .getBytes())).toString(16);
      // Hash string has no leading zeros
      // Adding zeros to the beginnig of has string
      while (hash_text.length() < 32) {
        hash_text = "0" + hash_text;
      }
      lockFile = new File(tmp_dir + hash_text + ".app_lock");
    } catch (Exception ex) {
      LOG.error("AppLock.AppLock() file fail", ex);
    }

    // MD5 acquire fail
    if (lockFile == null) {
      lockFile = new File(tmp_dir + key + ".app_lock");
    }

    lockStream = new FileOutputStream(lockFile);

    final String f_content = "Java AppLock Object\r\nLocked by key: " + key
        + "\r\n";
    lockStream.write(f_content.getBytes());

    lockChannel = lockStream.getChannel();

    lock = lockChannel.tryLock();

    if (lock == null) {
      throw new IngestRuntimeException("Can't create Lock");
    }
  }

  /**
   * Release Lock. Now another application instance can gain lock.
   */
  private void release() throws Throwable {
    if (lock.isValid()) {
      lock.release();
    }
    if (lockStream != null) {
      lockStream.close();
    }
    if (lockChannel.isOpen()) {
      lockChannel.close();
    }
    if (lockFile.exists()) {
      lockFile.delete();
    }
  }

  @Override
  protected void finalize() throws Throwable {
    this.release();
    super.finalize();
  }


  /**
   * Set application lock. Method can be run only one time per application. All next calls will be ignored.
   *
   * @param key Unique application lock key
   * @return true, if successful
   */
  public static boolean setLock(String key) {
    if (instance != null) {
      return true;
    }

    try {
      instance = new AppLock(key);
    } catch (Exception ex) {
      instance = null;
      LOG.error("Fail to set AppLoc", ex);
      return false;
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        AppLock.releaseLock();
      }
    });
    return true;
  }

  /**
   * Trying to release Lock. After release you can not user AppLock again in this application.
   */
  public static void releaseLock() {
    try {
      if (instance == null) {
        throw new NoSuchFieldException("INSTANCE IS NULL");
      }
      instance.release();
    } catch (Throwable ex) {
      LOG.error("Fail to release lock", ex);
    }
  }

  private static AppLock instance;
  private static final Logger LOG = LoggerFactory.getLogger(AppLock.class);

}