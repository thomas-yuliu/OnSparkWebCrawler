package baozi.webcrawler.onspark.common.entry;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.urlfilter.ContentTypeFilter;
import baozi.webcrawler.common.urlfilter.InMemroySeenUrlFilter;
import baozi.webcrawler.common.urlfilter.UrlDepthFilter;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.onspark.common.analyzer.NaiveBayesBasedOfferPageAnalyzer;

public class ConfigLoader {
  private LogManager logger = new LogManager(ConfigLoader.class);
  private JSONObject jsonObject;
  
  private void loadInputConfigFile(String inputFilePath){
    JSONParser parser = new JSONParser();
    try {
      jsonObject = (JSONObject)parser.parse(new FileReader(inputFilePath));
    } catch (IOException | ParseException e) {
      logger.logError("error when parsing config file: " + inputFilePath + "; " + e.toString());
    }
  }
  
  public void load(){
  //TODO to use relative path of config file
    loadInputConfigFile("/Users/yliu/mavenWorkspace/eclipse-workspace/WebCrawler/conf/inputConfig.json");

    InMemroySeenUrlFilter seenFilter = new InMemroySeenUrlFilter();
    OnSparkInstanceFactory.getPostExpansionFilterEnforcer().addFilter(seenFilter);
    
    try {
      BaseURL baseUrl = null;
      List<BaseURL> nextUrls = new ArrayList<>();
      List<String> seedingUrls = (List<String>) jsonObject.get("seeding_url");
      for(String seed : seedingUrls){
        baseUrl = new BaseURL(new URL(seed));
        baseUrl.setDepthFromSeed(0);
        nextUrls.add(baseUrl);
        seenFilter.filter(baseUrl);
      }
      //BaseToCrawlUrls lbtcu = InstanceFactory.getOneBaseToCrawlUrlsInstance();
      //lbtcu.putToCrawlUrls(nextUrls);
      OnSparkInstanceFactory.getNextURLQueueInstance().putNextUrls(nextUrls);
      logger.logInfo("starting from baseUrl: " + nextUrls.toString());
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    UrlDepthFilter preFilter = new UrlDepthFilter(1);
    OnSparkInstanceFactory.getPreExpansionFilterEnforcer().addFilter(preFilter);
    
    ContentTypeFilter fileExtensionFilter = new ContentTypeFilter();
    OnSparkInstanceFactory.getPostExpansionFilterEnforcer().addFilter(fileExtensionFilter);
    
    NaiveBayesBasedOfferPageAnalyzer nb = new NaiveBayesBasedOfferPageAnalyzer();
    OnSparkInstanceFactory.getAnalyzingManager().addAnalyzer(nb);
  }
}
