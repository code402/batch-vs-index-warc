package com.code402;

import java.io.*;
import java.net.URL;
import org.apache.commons.compress.compressors.gzip.*;
import org.netpreserve.jwarc.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class Batch {
  static boolean USE_SSL = false;
  static int NUM_RECORDS = 1000 * 1000;
  static int NUM_CORES = 1;
  static LongAdder records = new LongAdder();
  static InputStream getStream(String url) throws Exception {
    return new GzipCompressorInputStream(new URL(url).openStream(), true);
  }

  static {
    if(System.getenv("NUM_RECORDS") != null)
      NUM_RECORDS = Integer.parseInt(System.getenv("NUM_RECORDS"));

    USE_SSL = System.getenv("USE_SSL") != null;

    if(System.getenv("NUM_CORES") != null)
      NUM_CORES = Integer.parseInt(System.getenv("NUM_CORES"));
  }

  static long startTime = System.currentTimeMillis();

  static String[] getWarcUrls(String warcsUrl, int howMany) throws Exception {
    InputStream is = getStream(warcsUrl);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      return br.lines().limit(howMany).toArray(String[]::new);
    }
  }

  static class ProcessWarc implements Runnable {
    String url;
    CountDownLatch latch;
    ProcessWarc(String url, CountDownLatch latch) {
      this.url = url;
      this.latch = latch;
    }

    public void run() {
      try {
        System.out.println(url);

        InputStream warcIs = getStream("http" + (USE_SSL ? "s" : "") + "://commoncrawl.s3.amazonaws.com/" + url);
        WarcReader reader = new WarcReader(warcIs);
        for(WarcRecord record : reader) {
          if(record instanceof WarcResponse) {
            records.increment();
          }

          if(records.sum() > NUM_RECORDS)
            break;
        }
      } catch(Exception e) {
        e.printStackTrace();
        System.exit(1);
      } finally {
        latch.countDown();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CountDownLatch latch = new CountDownLatch(NUM_CORES);

    Thread printer = new Thread() {
      public void run() {
        while(true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
          }
          printRate();
        }
      }
    };

    printer.start();

    String[] urls = getWarcUrls(
      "http" + (USE_SSL ? "s" : "") + "://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-30/warc.paths.gz",
      NUM_CORES
    );

    ThreadPoolExecutor pool =
      new ThreadPoolExecutor(NUM_CORES, NUM_CORES, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
    // Start multiple threads -- can do this later, for now, do it in proc.
    long start = System.currentTimeMillis();
    for(String url: urls) {
      ProcessWarc pw = new ProcessWarc(url, latch);
      pool.submit(pw);
    }

    latch.await();

    printRate();
    System.exit(0);
  }

  static void printRate() {
    long elapsed = System.currentTimeMillis() - startTime;
    System.out.println("Read " + records.sum() + " records in " + elapsed + "ms, " + (records.doubleValue() / (elapsed / 1000.0)) + " records/sec.");
  }
}
