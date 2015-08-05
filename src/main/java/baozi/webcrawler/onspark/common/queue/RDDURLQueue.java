package baozi.webcrawler.onspark.common.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.spark.api.java.JavaRDD;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.onspark.common.entry.OnSparkInstanceFactory;
import baozi.webcrawler.onspark.common.workflow.OnSparkWorkflowManager;

public class RDDURLQueue {
  private LogManager logger = new LogManager(RDDURLQueue.class);

  private BlockingQueue<BaseURL> queue = new LinkedBlockingQueue<>();
  private static final int batchsize = 100;
  
  public JavaRDD<BaseURL> nextBatch() {
    List<BaseURL> collection = new ArrayList<>();
    queue.drainTo(collection, batchsize);
    logger.logDebug("putting next batch into RDD: " + collection.toString());
    return OnSparkInstanceFactory.getSparkContext().parallelize(collection, 2);
  }

  public boolean hasMoreUrls() {
    return !queue.isEmpty();
  }

  public void putNextUrls(List<BaseURL> nextUrls) {
    queue.addAll(nextUrls);
  }

}
