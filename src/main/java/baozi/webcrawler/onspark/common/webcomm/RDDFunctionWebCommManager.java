package baozi.webcrawler.onspark.common.webcomm;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;

import baozi.webcrawler.common.entry.InstanceFactory;
import baozi.webcrawler.common.metainfo.BaseURL;

public class RDDFunctionWebCommManager implements Serializable{


  /**
   * 
   */
  private static final long serialVersionUID = -4566694261803813944L;

  public static Function<BaseURL, BaseURL> downloadPageContent() {
    Function<BaseURL, BaseURL> result = new Function<BaseURL, BaseURL>(){
      public BaseURL call(BaseURL base){
        base.downloadPageContent(InstanceFactory.getWebCommManager());
        return base;
      }
    };
    return result;
  }

  public static Function<BaseURL, Boolean> filterEmptyUrls() {
    Function<BaseURL, Boolean> result = new Function<BaseURL, Boolean>(){
      public Boolean call(BaseURL base){
        return base.isValid();
      }
    };
    return result;
  }

}
