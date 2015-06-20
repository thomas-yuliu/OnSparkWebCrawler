package baozi.webcrawler.onspark.common.webcomm;

import org.apache.spark.api.java.function.Function;

import baozi.webcrawler.common.entry.InstanceFactory;
import baozi.webcrawler.common.metainfo.BaseURL;

public class RDDFunctionWebCommManager {

  public Function<BaseURL, BaseURL> downloadPageContent() {
    Function<BaseURL, BaseURL> result = new Function<BaseURL, BaseURL>(){
      public BaseURL call(BaseURL base){
        base.downloadPageContent(InstanceFactory.getWebCommManager());
        return base;
      }
    };
    return result;
  }

  public Function<BaseURL, Boolean> filterEmptyUrls() {
    Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>(){
      public Boolean call(BaseURL base){
        return base.isValid();
      }
    };
    return result;
  }

}
