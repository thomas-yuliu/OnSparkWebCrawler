package baozi.webcrawler.onspark.common.entry;

import baozi.webcrawler.onspark.common.analyzer.MLJsoupRDDAnalyzer;

public class CommandlineKicker extends OnSparkKicker {

  public static void main(String[] input){
    OnSparkKicker k = new OnSparkKicker();
    //k.kick();
    MLJsoupRDDAnalyzer ml = new MLJsoupRDDAnalyzer();
    ml.train();
  }
}
