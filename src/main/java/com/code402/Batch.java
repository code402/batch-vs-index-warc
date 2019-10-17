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
    ProcessWarc(String url) {
      this.url = url;
    }

    public void run() {
      try {
        System.out.println(url);

        byte[] buffer = new byte[2048];
        InputStream warcIs = getStream("http" + (USE_SSL ? "s" : "") + "://commoncrawl.s3.amazonaws.com/" + url);
        WarcReader reader = new WarcReader(warcIs);
        for(WarcRecord record : reader) {
          if(record instanceof WarcResponse) {
            records.increment();
            //consume the body
            WarcResponse response = (WarcResponse)record;
            InputStream is = response.body().stream();
            while(true) {
              int read = is.read(buffer);
              if(read == -1)
                break;
            }
            is.close();

          }

          if(records.sum() > NUM_RECORDS)
            break;
        }
      } catch(Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  public static void main(String[] args) throws Exception {
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
      4 * NUM_CORES
    );

    ThreadPoolExecutor pool =
      new ThreadPoolExecutor(NUM_CORES, NUM_CORES, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
    // Start multiple threads -- can do this later, for now, do it in proc.
    long start = System.currentTimeMillis();
    for(String url: urls) {
      ProcessWarc pw = new ProcessWarc(url);
      pool.submit(pw);
    }


    Thread.sleep(65000);
    printRate();
    System.exit(0);
  }

  static void printRate() {
    long elapsed = System.currentTimeMillis() - startTime;
    System.out.println("Read " + records.sum() + " records in " + elapsed + "ms, " + (records.doubleValue() / (elapsed / 1000.0)) + " records/sec.");
  }
}
