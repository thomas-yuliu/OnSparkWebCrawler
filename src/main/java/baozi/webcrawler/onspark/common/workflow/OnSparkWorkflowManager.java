package baozi.webcrawler.onspark.common.workflow;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import baozi.webcralwer.common.utils.LogManager;
import baozi.webcralwer.common.utils.PaceKeeper;
import baozi.webcrawler.onspark.common.analyzer.RDDAnalyzer;
import baozi.webcrawler.onspark.common.entry.InstanceFactory;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.queue.URLQueue;
import baozi.webcrawler.common.urlfilter.PostExpansionFilterEnforcer;
import baozi.webcrawler.common.urlfilter.PreExpansionFilterEnforcer;
import baozi.webcrawler.common.urlidentifier.URLIdentifier;
import baozi.webcrawler.common.webcomm.HTTPWebCommManager;
import baozi.webcrawler.onspark.common.queue.RDDURLQueue;
import baozi.webcrawler.onspark.common.urlfilter.RDDPostExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlfilter.RDDPreExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlidentifier.RDDURLIdentifier;
import baozi.webcrawler.onspark.common.webcomm.RDDFunctionWebCommManager;

public class OnSparkWorkflowManager {
  private LogManager logger = new LogManager(OnSparkWorkflowManager.class);

  private RDDURLQueue nextQueue = InstanceFactory.getNextURLQueueInstance();
  private RDDFunctionWebCommManager rddFunctionWebCommManager = InstanceFactory.getRDDFunctionWebCommManager();
  private RDDAnalyzer rddAnalyzer = InstanceFactory.getRDDAnalyzer();
  private RDDURLIdentifier urlIdentifier = InstanceFactory.getURLIdentifier();
  private RDDPreExpansionFilterEnforcer preExpansionfilterEnforcer = InstanceFactory.getPreExpansionFilterEnforcer();
  private RDDPostExpansionFilterEnforcer postExpansionfilterEnforcer = InstanceFactory.getPostExpansionFilterEnforcer();

  public void crawl() {

    while (shouldContinue()) {
      JavaRDD<String> currBatch = nextQueue.nextBatch();
      JavaRDD<BaseURL> downloaded = currBatch
          .map(rddFunctionWebCommManager.downloadPageContent())
          .filter(rddFunctionWebCommManager.filterEmptyUrls()).cache();
      downloaded.foreach(rddAnalyzer.analyze());
      List<BaseURL> nextUrls = downloaded
          .filter(preExpansionfilterEnforcer.applyFilters())
          .flatMap(urlIdentifier.extractUrls())
          .filter(postExpansionfilterEnforcer.applyFilters()).collect();
      nextQueue.putNextUrls(nextUrls);

      PaceKeeper.pause();
    }
  }

  protected boolean shouldContinue(){
    logger.logInfo("should continue: " + nextQueue.hasMoreUrls());
    return nextQueue.hasMoreUrls();
  }
}
