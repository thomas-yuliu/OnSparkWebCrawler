package baozi.webcrawler.onspark.common.entry;

import baozi.webcrawler.onspark.common.analyzer.NaiveBayesPageClassifier;

public class CommandlineKicker extends OnSparkKicker {

  public static void main(String[] input){
    OnSparkKicker k = new OnSparkKicker();
    //k.kick();
    NaiveBayesPageClassifier ml = new NaiveBayesPageClassifier();
    ml.train();
    ml.predict();
  }
}
