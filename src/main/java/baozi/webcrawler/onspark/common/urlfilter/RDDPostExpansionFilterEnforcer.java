package baozi.webcrawler.onspark.common.urlfilter;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlfilter.ContentTypeFilter;
import baozi.webcrawler.common.urlfilter.FilterEnforcer;
import baozi.webcrawler.common.urlfilter.InMemroySeenUrlFilter;
import baozi.webcrawler.common.utils.LogManager;

public class RDDPostExpansionFilterEnforcer extends FilterEnforcer implements Serializable{
  private static transient LogManager logger = new LogManager(RDDPostExpansionFilterEnforcer.class);

  /**
   * 
   */
  private static final long serialVersionUID = 2710224297826136116L;

  public Function<BaseURL, Boolean> filter() {
    RDDPostExpansionFilterEnforcer enf = this;
    Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>(){
      public Boolean call(BaseURL base){
        boolean result = applyFilters(base);
        return result;
      }
    };
    return result;
  }
}
