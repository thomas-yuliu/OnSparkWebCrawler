package baozi.webcrawler.onspark.common.analyzer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import scala.Tuple2;
import baozi.webcralwer.common.utils.LogManager;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.metainfo.JsoupDocWebPage;
import baozi.webcrawler.common.webcomm.JsoupWebCommManager;
import baozi.webcrawler.onspark.common.workflow.OnSparkWorkflowManager;

public class MLJsoupRDDAnalyzer {
  private static transient LogManager logger = new LogManager(
      OnSparkWorkflowManager.class);

  public void train() {
    // download jsoup is not needed
    URL url = null;
    try {
      url = new URL("http://www.mitbbs.com/article_t/JobHunting/32877425.html");
    } catch (MalformedURLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    BaseURL baseUrl = new BaseURL(url);
    JsoupWebCommManager jwcm = new JsoupWebCommManager();
    baseUrl.downloadPageContent(jwcm);
    JsoupDocWebPage jdwp = (JsoupDocWebPage) baseUrl.getPageContent();
    // logger.logDebug("jdwp: " + jdwp.getPageHtml());
    // need a class hierarchy. this is specific to mitbbs
    Elements elements = jdwp.getDoc().body().getElementsByTag("p");
    // logger.logDebug("ele: " + elements.toString());
    Iterator<Element> itr = elements.iterator();
    StringBuilder sb = new StringBuilder();
    while (itr.hasNext()) {
      Element element = itr.next();
      sb.append(element.outerHtml());
    }
    // need tokenization here
    LinkedList<String> charArray = new LinkedList<>();
    Set<Character> dontwantthem = new HashSet<>(Arrays.asList('<', '>', '(',
        ')', ',', '?', '/', ';'));
    for (int i = 0; i < sb.length(); i++) {
      logger.logDebug(String.valueOf(sb.charAt(i)));
      if (!dontwantthem.contains(sb.charAt(i))) {
        charArray.add(String.valueOf(sb.charAt(i)));
      }
    }
    logger.logDebug("done: " + charArray);

    int totalnumOfDoc = 0;
    int numOfOffer = 0;
    int numOfNotOffer = 0;
    double offerPoss = numOfOffer / totalnumOfDoc;
    double notofferPoss = numOfNotOffer / totalnumOfDoc;

  }

  public Function<LinkedList<String>, HashMap<String, ArrayList<Integer>>> countTimesInEachDocument() {
    return new Function<LinkedList<String>, HashMap<String, ArrayList<Integer>>>() {
      @Override
      public HashMap<String, ArrayList<Integer>> call(LinkedList<String> input)
          throws Exception {

        HashMap<String, ArrayList<Integer>> dictionary = new HashMap<String, ArrayList<Integer>>();
        for (String curr : input) {
          if (dictionary.containsKey(curr)) {
            ArrayList<Integer> value = dictionary.get(curr);
            value.set(0, value.get(0) + 1);
            dictionary.put(curr, value);
          } else {
            ArrayList<Integer> newPair = new ArrayList<Integer>();
            newPair.set(0, 1);
            dictionary.put(curr, newPair);
          }
        }
        return dictionary;
      }
    };
  }

  public FlatMapFunction<HashMap<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>> flatDictionary() {
    return new FlatMapFunction<HashMap<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>>() {

      @Override
      public Iterable<Tuple2<String, ArrayList<Integer>>> call(
          HashMap<String, ArrayList<Integer>> t) throws Exception {
        List<Tuple2<String, ArrayList<Integer>>> result = new ArrayList<Tuple2<String, ArrayList<Integer>>>();
        for (Entry<String, ArrayList<Integer>> entry : t.entrySet()) {
          Tuple2<String, ArrayList<Integer>> itr = new Tuple2<String, ArrayList<Integer>>(
              entry.getKey(), entry.getValue());
          result.add(itr);
        }
        return result;
      }
    };
  }
  

  public Function2<Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>> mergeDictionary() {
    return new Function2<Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>, Tuple2<String, ArrayList<Integer>>>() {

      @Override
      public Tuple2<String, ArrayList<Integer>> call(
          Tuple2<String, ArrayList<Integer>> arg0,
          Tuple2<String, ArrayList<Integer>> arg1) {
        
        ArrayList<Integer> resultList = new ArrayList<Integer>();
        for(int i = 0; i < arg0._2.size(); i++){
          resultList.set(i, arg0._2.get(i)+arg1._2.get(i));
        }
        
        return new Tuple2<String, ArrayList<Integer>>(arg0._1,  resultList);
      }
    };
  }
}
