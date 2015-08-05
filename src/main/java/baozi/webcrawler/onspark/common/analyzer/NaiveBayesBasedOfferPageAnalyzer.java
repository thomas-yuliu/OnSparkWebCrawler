package baozi.webcrawler.onspark.common.analyzer;

import java.io.Serializable;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlfilter.FilterEnforcer;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.offerpage.analyzer.OfferPageAnalyzer;
import baozi.webcrawler.offerpage.entry.InstanceFactory;
import baozi.webcrawler.offerpage.offerpagestorage.OfferPageStorage;

public class NaiveBayesBasedOfferPageAnalyzer extends OfferPageAnalyzer implements Serializable{

  private static transient LogManager logger = new LogManager(NaiveBayesBasedOfferPageAnalyzer.class);

  private NaiveBayesPageClassifier nbPageClassifier;

  public NaiveBayesBasedOfferPageAnalyzer() {
    nbPageClassifier = new NaiveBayesPageClassifier();
    // TODO should we train the model implicitly here in constructor?
    nbPageClassifier.train();
  }

  @Override
  public boolean isAnOfferPage(BaseURL url) {

    double result = nbPageClassifier.predict();
    logger.logInfo("page " + url.toString() + " is categorized as: " + result);

    // TODO should we hard coding 1,0? we need to make a dictionary
    // if result is nearer to 1, then it is an offer page
    if (Math.abs(result - 1) < Math.abs(result - 0)) {
      return true;
    } else {
      return false;
    }
  }
}
