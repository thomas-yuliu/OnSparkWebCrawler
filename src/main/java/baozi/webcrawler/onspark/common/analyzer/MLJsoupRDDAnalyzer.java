package baozi.webcrawler.onspark.common.analyzer;

import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import scala.Tuple2;
import baozi.webcralwer.common.utils.LogManager;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.metainfo.JsoupDocWebPage;
import baozi.webcrawler.common.webcomm.JsoupWebCommManager;
import baozi.webcrawler.onspark.common.entry.OnSparkInstanceFactory;
import baozi.webcrawler.onspark.common.workflow.OnSparkWorkflowManager;

public class MLJsoupRDDAnalyzer {
  private static transient LogManager logger = new LogManager(
      OnSparkWorkflowManager.class);
  private transient JSONObject jsonObject;
  private transient NaiveBayes naiveBayes;
  private transient NaiveBayesModel naiveBayesModel;

  public void train() {
    loadTrainingDataFile(null);
    List<String> training_data = (List<String>) jsonObject.get("training_data");
    JavaPairRDD<String, Integer> training_rdd = OnSparkInstanceFactory
        .getSparkContext()
        .parallelize(training_data)
        .mapToPair(line -> {
          // Tuple2<String, Integer>: page url, category
            return new Tuple2<String, Integer>(line.split("\\p")[0], Integer
                .valueOf(line.split("\\p")[1]));
          });
    int totalnumOfDoc = training_data.size();
    JavaPairRDD<String, ArrayList<Integer>> intermediate = training_rdd.map(fetchAndConvert()).map(countTimesInEachDocument()).flatMap(flatDictionary()).mapToPair(convertToPair()).reduceByKey(mergeDictionary());
  }

  private void loadTrainingDataFile(String inputFilePath) {
    JSONParser parser = new JSONParser();
    try {
      jsonObject = (JSONObject) parser.parse(new FileReader(inputFilePath));
    } catch (IOException | ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /*
   * input: Tuple2<String, Integer>: page URL, category return
   * Tuple2<LinkedList<String>: tokenized words on the page , Integer>: category
   */
  public Function<Tuple2<String, Integer>, Tuple2<LinkedList<String>, Integer>> fetchAndConvert() {
    return new Function<Tuple2<String, Integer>, Tuple2<LinkedList<String>, Integer>>() {

      @Override
      public Tuple2<LinkedList<String>, Integer> call(
          Tuple2<String, Integer> input) throws Exception {
        // download jsoup is not needed
        URL url = null;
        try {
          url = new URL(input._1);
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
        Set<Character> dontwantthem = new HashSet<>(Arrays.asList('<', '>',
            '(', ')', ',', '?', '/', ';'));
        for (int i = 0; i < sb.length(); i++) {
          logger.logDebug(String.valueOf(sb.charAt(i)));
          if (!dontwantthem.contains(sb.charAt(i))) {
            charArray.add(String.valueOf(sb.charAt(i)));
          }
        }
        logger.logDebug("done: " + charArray);

        return new Tuple2<LinkedList<String>, Integer>(charArray, input._2);
      }
    };
  }

  /*
   * input: Tuple2<LinkedList<String>: tokenized words on the page , Integer>:
   * category return: HashMap<String, ArrayList<Integer>>: word -> appeared
   * times for each category(as array)
   */
  public Function<Tuple2<LinkedList<String>, Integer>, HashMap<String, ArrayList<Integer>>> countTimesInEachDocument() {
    return new Function<Tuple2<LinkedList<String>, Integer>, HashMap<String, ArrayList<Integer>>>() {
      @Override
      public HashMap<String, ArrayList<Integer>> call(
          Tuple2<LinkedList<String>, Integer> input) throws Exception {

        HashMap<String, ArrayList<Integer>> dictionary = new HashMap<String, ArrayList<Integer>>();
        for (String curr : input._1) {
          if (dictionary.containsKey(curr)) {
            ArrayList<Integer> value = dictionary.get(curr);
            value.set(input._2(), value.get(input._2()) + 1);
            dictionary.put(curr, value);
          } else {
            ArrayList<Integer> newPair = new ArrayList<Integer>();
            newPair.set(input._2(), 1);
            dictionary.put(curr, newPair);
          }
        }
        return dictionary;
      }
    };
  }

  /*
   * input: HashMap<String, ArrayList<Integer>>: word -> appeared times for each
   * category(as array) return: Tuple2<String, ArrayList<Integer>>: a iterable
   * list of word -> appeared times pairs
   */
  public FlatMapFunction<HashMap<String, ArrayList<Integer>>, Entry<String, ArrayList<Integer>>> flatDictionary() {
    return new FlatMapFunction<HashMap<String, ArrayList<Integer>>, Entry<String, ArrayList<Integer>>>() {

      @Override
      public Iterable<Entry<String, ArrayList<Integer>>> call(
          HashMap<String, ArrayList<Integer>> t) throws Exception {
        List<Entry<String, ArrayList<Integer>>> result = new ArrayList<Entry<String, ArrayList<Integer>>>();
        for (Entry<String, ArrayList<Integer>> entry : t.entrySet()) {
          Entry<String, ArrayList<Integer>> itr = new AbstractMap.SimpleEntry<String, ArrayList<Integer>>(
              entry.getKey(), entry.getValue());
          result.add(itr);
        }
        return result;
      }
    };
  }
  
  public PairFunction<Entry<String, ArrayList<Integer>>, String, ArrayList<Integer>> convertToPair(){
    return new PairFunction<Entry<String, ArrayList<Integer>>, String, ArrayList<Integer>>(){

      @Override
      public Tuple2<String, ArrayList<Integer>> call(
          Entry<String, ArrayList<Integer>> t) throws Exception {
        return new Tuple2<String, ArrayList<Integer>>(t.getKey(), t.getValue());
      }
      
    };
  }

  /*
   * input: 2 of this: Tuple2<String, ArrayList<Integer>>: a iterable list of
   * word -> appeared times pairs return: Tuple2<String, ArrayList<Integer>>:
   * merged the 2 lists of word -> appeared times pairs
   */
  public Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>> mergeDictionary() {
    return new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {

      @Override
      public ArrayList<Integer> call(ArrayList<Integer> v1,
          ArrayList<Integer> v2) throws Exception {

        ArrayList<Integer> resultList = new ArrayList<Integer>();
        for (int i = 0; i < v1.size(); i++) {
          resultList.set(i, v2.get(i) + v1.get(i));
        }

        return resultList;
      }
    };
  }
}
