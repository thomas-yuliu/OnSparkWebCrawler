package baozi.webcrawler.onspark.common.workflow;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import baozi.webcrawler.onspark.common.analyzer.RDDAnalyzingManager;
import baozi.webcrawler.onspark.common.entry.OnSparkInstanceFactory;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.common.utils.PaceKeeper;
import baozi.webcrawler.onspark.common.queue.RDDURLQueue;
import baozi.webcrawler.onspark.common.urlfilter.RDDPostExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlfilter.RDDPreExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlidentifier.RDDURLIdentifier;
import baozi.webcrawler.onspark.common.webcomm.RDDFunctionWebCommManager;

public class OnSparkWorkflowManager {
  private LogManager logger = new LogManager(OnSparkWorkflowManager.class);

  private RDDURLQueue nextQueue = OnSparkInstanceFactory.getNextURLQueueInstance();
  private RDDAnalyzingManager rddAnalManager = OnSparkInstanceFactory.getAnalyzingManager();
  private RDDURLIdentifier urlIdentifier = OnSparkInstanceFactory.getURLIdentifier();
  private RDDPreExpansionFilterEnforcer preExpansionfilterEnforcer = OnSparkInstanceFactory.getPreExpansionFilterEnforcer();
  private RDDPostExpansionFilterEnforcer postExpansionfilterEnforcer = OnSparkInstanceFactory.getPostExpansionFilterEnforcer();

  public void crawl() {
    while (shouldContinue()) {
      JavaRDD<BaseURL> currBatch = nextQueue.nextBatch();
      
      currBatch.cache();
      //logger.logDebug("received next batch from queue: " + currBatch.toArray().toString());
      JavaRDD<BaseURL> downloaded = currBatch
          .map(RDDFunctionWebCommManager.downloadPageContent())
          .filter(RDDFunctionWebCommManager.filterEmptyUrls()).cache();
      downloaded.cache();
      logger.logDebug("downloaded web pages");
      downloaded.foreach(rddAnalManager.manageAnalyzing());

      List<BaseURL> nextUrls = downloaded
          .filter(preExpansionfilterEnforcer.filter())
          .flatMap(urlIdentifier.extractUrls())
          .filter(postExpansionfilterEnforcer.filter()).collect();
      logger.logDebug("next batches: " + nextUrls.toString());
      nextQueue.putNextUrls(nextUrls);
      
      PaceKeeper.pause();
    }
  }

  protected boolean shouldContinue(){
    logger.logInfo("should continue: " + nextQueue.hasMoreUrls());
    return nextQueue.hasMoreUrls();
  }
}
