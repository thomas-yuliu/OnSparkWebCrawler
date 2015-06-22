package baozi.webcrawler.onspark.common.analyzer;

import java.io.Serializable;

import org.apache.spark.api.java.function.VoidFunction;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.offerpage.analyzer.JsoupBasedOfferPageAnalyzer;

public class RDDAnalyzer implements Serializable{

  /**
   * 
   */
  private static final long serialVersionUID = 9206242937757482763L;

  public VoidFunction<BaseURL> analyze() {
    VoidFunction<BaseURL> result = new VoidFunction<BaseURL>(){
      public void call(BaseURL base){
        JsoupBasedOfferPageAnalyzer analyzer = new JsoupBasedOfferPageAnalyzer();
        analyzer.analyze(base);
      }
    };
    return result;
  }
}
