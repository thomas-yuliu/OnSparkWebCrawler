package baozi.webcrawler.onspark.common.urlfilter;

import org.apache.spark.api.java.function.Function;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlfilter.ContentTypeFilter;
import baozi.webcrawler.common.urlfilter.FilterEnforcer;
import baozi.webcrawler.common.urlfilter.InMemroySeenUrlFilter;

public class RDDPostExpansionFilterEnforcer extends FilterEnforcer{

  public Function<BaseURL, Boolean> filter() {
    Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>(){
      public Boolean call(BaseURL base){
        return applyFilters(base);
      }
    };
    return result;
  }
}
