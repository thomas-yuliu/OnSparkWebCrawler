package baozi.webcrawler.onspark.common.analyzer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import scala.Tuple2;
import baozi.webcrawler.common.entry.InstanceFactory;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.metainfo.JsoupDocWebPage;
import baozi.webcrawler.common.tokenizer.TextTokenizer;
import baozi.webcrawler.common.utils.LogManager;
import baozi.webcrawler.common.webcomm.JsoupWebCommManager;
import baozi.webcrawler.onspark.common.entry.OnSparkInstanceFactory;

import com.clearspring.analytics.util.Lists;

public class NaiveBayesPageClassifier implements Serializable {
  private static transient LogManager logger = new LogManager(
      NaiveBayesPageClassifier.class);
  private Map<String, Integer> docToClass;
  private Map<String, List<String>> docToTerms;
  private Map<String, Double> termToIdf;
  private Map<String, Integer> termToIndex = new HashMap<>();
  private JavaRDD<LabeledPoint> listOfLabledPoint;
  private NaiveBayesModel naiveBayesModel;

  //train the model
  public void train() {
    //TODO to use relative path of config file
    loadTrainingDataFile("/Users/yliu/mavenWorkspace/eclipse-workspace/OnSparkWebCrawler/conf/training_data.json");
    logger.logDebug("training urls: " + docToClass.toString());
    JavaPairRDD<String, List<String>> docToTermsRDD = fetchDocTerms(docToClass).cache();
    docToTerms = docToTermsRDD.collectAsMap();
    List<Tuple2<String, List<String>>> docToTermsList = docToTermsRDD.collect();
    logger.logDebug("docToTermsList: " + docToTermsList.toString());
    termToIdf = calculateIDF(docToClass, docToTermsList);
    logger.logDebug("termToIdf size: " + termToIdf.size() + " ; termToIdf: " + termToIdf.toString());
    
    // TODO think about whether below can be parallelized
    int assigned_index = 0;
    for(String key : termToIdf.keySet()){
      termToIndex.put(key, assigned_index);
      assigned_index ++;
    }
    logger.logDebug("termToIndex: " + termToIndex.size() + " ; term to index: " + termToIndex.toString());
    
    listOfLabledPoint = calculateLabeledPoints(docToClass, docToTermsList,
        termToIdf);
    naiveBayesModel = NaiveBayes.train(listOfLabledPoint.rdd());
    logger.logInfo("trained model.");
  }

  //predict the category of the page
  public double predict() {
    Map<String, Integer> docToClass = new HashMap<String, Integer>();
    docToClass.put("http://www.mitbbs.com/article_t/JobHunting/33013183.html",
        -1);// -1 not important. just to use the map
    JavaPairRDD<String, List<String>> docToTermsRDD = fetchDocTerms(docToClass).cache();
    Map<String, List<String>> docToTerms = docToTermsRDD.collectAsMap();
    List<Tuple2<String, List<String>>> docToTermsList = docToTermsRDD.collect();
    //Map<String, Double> termToIdf = calculateIDF(docToClass, docToTermsList);
    JavaRDD<LabeledPoint> listOfLabledPoint = calculateLabeledPoints(docToClass, docToTermsList,
        termToIdf);
    LabeledPoint lp = listOfLabledPoint.collect().get(0);
    logger.logDebug("lp before predict: " + lp.toString());
    double predict_result = naiveBayesModel.predict(lp.features());
    logger.logInfo("result is: " + predict_result);
    
    // do this: https://github.com/fommil/netlib-java/issues/62
    return predict_result;
  }

  private JavaRDD<LabeledPoint> calculateLabeledPoints(
      Map<String, Integer> docToClass, List<Tuple2<String, List<String>>> docToTerms,
      Map<String, Double> termToIdf) {
    // TODO dont run as divided jobs, but optimize the workflow
    JavaRDD<Tuple2<String, List<String>>> docToTermRdd = OnSparkInstanceFactory
        .getSparkContext().parallelize(docToTerms);
    return docToTermRdd
        .map(docToLabeledPoint(docToClass, termToIdf));
  }

  private Function<Tuple2<String, List<String>>, LabeledPoint> docToLabeledPoint(
      Map<String, Integer> docToClass,
      Map<String, Double> termToIdf) {
    return new Function<Tuple2<String, List<String>>, LabeledPoint>() {

      // TODO what is the index of string, what order we need to follow when
      // creating the labeledpoint?

      @Override
      // param: doc -> terms it contains
      public LabeledPoint call(Tuple2<String, List<String>> input)
          throws Exception {
        // iterate through all terms, calculate each's occurrence times
        LinkedHashMap<String, Integer> seen = new LinkedHashMap<>();
        for (String itr : input._2) {
          if (seen.containsKey(itr)) {
            seen.put(itr, seen.get(itr) + 1);
          } else {
            seen.put(itr, 1);
          }
        }
        LinkedList<Tuple2<Integer, Double>> linkedlist = new LinkedList<>();
        for (Entry<String, Integer> itr : seen.entrySet()) {
          
          //when used by prediction, it is possible the word is not in dictionary yet
          //for now, ignore. TODO think about what to do
          if(!termToIndex.containsKey(itr.getKey())){continue;}
          
          logger.logDebug("string is: " + itr.getKey() + " tf-idf is: " + (double) (termToIdf.get(itr.getKey()) * itr.getValue() / seen.size()));
          
          linkedlist.add(new Tuple2<Integer, Double>(termToIndex.get(itr.getKey()),
              (double) (termToIdf.get(itr.getKey()) * itr.getValue() / seen.size())));
        }
        String key = input._1;
        //weird int -> str and then back because of long -> integer casting exception otherwise.
        String key_str = String.valueOf(docToClass.get(key));
        int label = Integer.parseInt(key_str);
        logger.logDebug("label: " + label + " ; vec: " + linkedlist.toString() + " ; size: " + linkedlist.size());
        Vector vec = Vectors.sparse(termToIndex.size(), linkedlist);
        return new LabeledPoint(label, vec);
      }
    };
  }

  private Map<String, Double> calculateIDF(Map<String, Integer> docToClass,
      List<Tuple2<String, List<String>>> docToTerms) {
    JavaRDD<Tuple2<String, List<String>>> docToTermRdd = OnSparkInstanceFactory
        .getSparkContext().parallelize(docToTerms);
    JavaPairRDD<String, String> imm1 = docToTermRdd.flatMapToPair(
        termListsToRddOfTerms()).distinct()
        // just to convert entry<doc, terms it contains> to a list of <doc, term> for all terms it contains
        // make sure to distinct() because same term multi time is allowed. 
        // and for here we don't care how many times a term appears
        // we only care how many docs contain the term
        .cache();
    JavaPairRDD<String, Iterable<Tuple2<String, String>>> imm2 = imm1
        .groupBy(tuple -> tuple._2).cache();
    logger.logDebug("term to list of doc: " + imm2.collectAsMap().toString());
    // group by term and convert the list of <doc, term> to <term, iterable
    // of docs containing the term>
    Map<String, Double> result = imm2.mapToPair(
        entry -> new Tuple2<String, Double>(entry._1, (double) (docToClass
            .size() / Lists.newArrayList(entry._2).size()))).collectAsMap();
    // convert <term, iterable of docs containing the term> into <term, total num of docs/iterable.size()>
    return result;
  }

  //input is doc -> the list of terms it contains
  //output is list of doc to each terms it contains
  private PairFlatMapFunction<Tuple2<String, List<String>>, String, String> termListsToRddOfTerms() {
    return new PairFlatMapFunction<Tuple2<String, List<String>>, String, String>() {

      @Override
      public Iterable<Tuple2<String, String>> call(
          Tuple2<String, List<String>> input) throws Exception {
        logger.logDebug("input: " + input.toString());
        LinkedList<Tuple2<String, String>> result = new LinkedList<Tuple2<String, String>>();
        for (String itr : input._2()) {
          result.add(new Tuple2<String, String>(input._1, itr));
        }
        return result;
      }
    };
  }

  private void loadTrainingDataFile(String inputFilePath) {
    JSONParser parser = new JSONParser();
    try {
      JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(
          inputFilePath));
      docToClass = (Map<String, Integer>) jsonObject.get("training_data");
    } catch (IOException | ParseException e) {
    }
  }

  private JavaPairRDD<String, List<String>> fetchDocTerms(
      Map<String, Integer> docToClass) {
    JavaRDD<String> allDocUrls = OnSparkInstanceFactory.getSparkContext()
        .parallelize(new ArrayList<String>(docToClass.keySet()));
    return allDocUrls.mapToPair(downloadPageAndTokenize());
  }

  /*
   * input: String: page URL, category return: Tuple2<String, List<String>>
   * String: url; List<String>: terms the url contains
   */
  private PairFunction<String, String, List<String>> downloadPageAndTokenize() {
    return new PairFunction<String, String, List<String>>() {

      @Override
      public Tuple2<String, List<String>> call(String docUrl) throws Exception {
        // download jsoup is not needed
        URL url = null;
        try {
          url = new URL(docUrl);
        } catch (MalformedURLException e) {
        }
        BaseURL baseUrl = new BaseURL(url);
        JsoupWebCommManager jwcm = new JsoupWebCommManager();
        baseUrl.downloadPageContent(jwcm);
        JsoupDocWebPage jdwp = (JsoupDocWebPage) baseUrl.getPageContent();
        // TODO need a class hierarchy. this is specific to mitbbs
        Elements elements = jdwp.getDoc().body().getElementsByTag("p");
        Iterator<Element> itr = elements.iterator();
        StringBuilder sb = new StringBuilder();
        while (itr.hasNext()) {
          Element element = itr.next();
          //logger.logDebug("ownText: " + element.ownText());
          sb.append(element.ownText());
        }
        
        TextTokenizer tokenizer = InstanceFactory.getTextTokenizer();
        List<String> stems = tokenizer.tokenize(sb.toString());
        
        // TODO need to make sure each char in charArray appears same times as
        // it appears in the doc
        // TODO need to make sure each char is in order as it appears in the doc

        return new Tuple2<String, List<String>>(docUrl, stems);
      }
    };
  }
}
