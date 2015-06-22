package baozi.webcrawler.onspark.common.entry;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import baozi.webcrawler.onspark.common.analyzer.RDDAnalyzer;
import baozi.webcrawler.onspark.common.queue.RDDURLQueue;
import baozi.webcrawler.onspark.common.urlfilter.RDDPostExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlfilter.RDDPreExpansionFilterEnforcer;
import baozi.webcrawler.onspark.common.urlidentifier.RDDURLIdentifier;
import baozi.webcrawler.onspark.common.webcomm.RDDFunctionWebCommManager;

//need to refactor. bad design: multi instance factory
public class OnSparkInstanceFactory {
  private static final SparkConf sparkConf = new SparkConf().setAppName("BaoziSparkWebCralwer");
  private static final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
  private static final RDDAnalyzer rddAnalyzer = new RDDAnalyzer();
  private static final RDDPreExpansionFilterEnforcer rddPreExpansionFilterEnforcer = new RDDPreExpansionFilterEnforcer();
  private static final RDDPostExpansionFilterEnforcer rddPostExpansionFilterEnforcer = new RDDPostExpansionFilterEnforcer();
  private static final RDDURLQueue rddURLQueue = new RDDURLQueue();
  private static final RDDURLIdentifier rddURLIdentifier = new RDDURLIdentifier();
  
  public static JavaSparkContext getSparkContext(){
    return sparkContext;
  }

  public static RDDAnalyzer getRDDAnalyzer() {
    return rddAnalyzer;
  }

  public static RDDPreExpansionFilterEnforcer getPreExpansionFilterEnforcer() {
    return rddPreExpansionFilterEnforcer;
  }

  public static RDDPostExpansionFilterEnforcer getPostExpansionFilterEnforcer() {
    return rddPostExpansionFilterEnforcer;
  }
  
  public static RDDURLQueue getNextURLQueueInstance() {
    return rddURLQueue;
  }

  public static RDDURLIdentifier getURLIdentifier() {
    return rddURLIdentifier;
  }
}
