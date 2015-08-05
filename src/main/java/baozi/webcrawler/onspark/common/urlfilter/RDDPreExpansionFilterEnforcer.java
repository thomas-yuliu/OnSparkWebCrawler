package baozi.webcrawler.onspark.common.urlfilter;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlfilter.FilterEnforcer;
import baozi.webcrawler.common.urlfilter.UrlDepthFilter;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.onspark.common.queue.RDDURLQueue;

public class RDDPreExpansionFilterEnforcer extends FilterEnforcer implements Serializable{
  private static transient LogManager logger = new LogManager(RDDPreExpansionFilterEnforcer.class);

  /**
   * 
   */
  private static final long serialVersionUID = -2356251362384822272L;

  public Function<BaseURL, Boolean> filter() {
    RDDPreExpansionFilterEnforcer enf = this;
    Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>(){
      public Boolean call(BaseURL base){
        boolean result = applyFilters(base);
        return result;
      }
    };
    return result;
  }
}
