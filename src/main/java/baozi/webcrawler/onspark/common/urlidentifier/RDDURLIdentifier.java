package baozi.webcrawler.onspark.common.urlidentifier;

import org.apache.spark.api.java.function.FlatMapFunction;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlidentifier.JsoupBasedURLIdentifier;

public class RDDURLIdentifier {

  public FlatMapFunction<BaseURL, BaseURL> extractUrls() {
    FlatMapFunction<BaseURL, BaseURL> result = new FlatMapFunction<BaseURL, BaseURL>(){
      @Override
      public Iterable<BaseURL> call(BaseURL base) throws Exception {
        JsoupBasedURLIdentifier iden = new JsoupBasedURLIdentifier();
        return iden.extractUrls(base);
      }
    };
    return result;
  }

}
