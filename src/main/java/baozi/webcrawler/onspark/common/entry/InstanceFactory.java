package baozi.webcrawler.onspark.common.entry;

import baozi.webcrawler.common.metainfo.BaseToCrawlUrls;
import baozi.webcrawler.onspark.common.analyzer.RDDAnalyzer;
import baozi.webcrawler.onspark.common.queue.RDDURLQueue;
import baozi.webcrawler.onspark.common.urlfilter.RDDPostExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlfilter.RDDPreExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlidentifier.RDDURLIdentifier;
import baozi.webcrawler.onspark.common.webcomm.RDDFunctionWebCommManager;

public class InstanceFactory {

  public static RDDFunctionWebCommManager getRDDFunctionWebCommManager() {
    // TODO Auto-generated method stub
    return null;
  }

  public static RDDAnalyzer getRDDAnalyzer() {
    // TODO Auto-generated method stub
    return null;
  }

  public static RDDPreExpansionFilterEnforcer getPreExpansionFilterEnforcer() {
    // TODO Auto-generated method stub
    return null;
  }

  public static RDDPostExpansionFilterEnforcer getPostExpansionFilterEnforcer() {
    // TODO Auto-generated method stub
    return null;
  }
  
  public static RDDURLQueue getNextURLQueueInstance() {
    return null;
  }

  public static RDDURLIdentifier getURLIdentifier() {
    // TODO Auto-generated method stub
    return null;
  }

  public static BaseToCrawlUrls getOneBaseToCrawlUrlsInstance() {
    // TODO Auto-generated method stub
    return null;
  }

}
