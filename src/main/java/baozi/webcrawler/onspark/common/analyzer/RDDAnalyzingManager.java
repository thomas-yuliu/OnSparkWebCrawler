package baozi.webcrawler.onspark.common.analyzer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.VoidFunction;

import baozi.webcrawler.common.analyzer.Analyzer;
import baozi.webcrawler.common.metainfo.BaseURL;

public class RDDAnalyzingManager implements Serializable{
  
  private List<Analyzer> analyzers = new ArrayList<Analyzer>();

  private static final long serialVersionUID = 9206242937757482763L;

  //call analyze() on all registered analyzers
  public VoidFunction<BaseURL> manageAnalyzing() {
    return new VoidFunction<BaseURL>(){
      public void call(BaseURL base){
        for(Analyzer analyzer : analyzers){
          analyzer.analyze(base);
        }
      }
    };
  }
  
  public void addAnalyzer(Analyzer analyzer){
    analyzers.add(analyzer);
  }
}
