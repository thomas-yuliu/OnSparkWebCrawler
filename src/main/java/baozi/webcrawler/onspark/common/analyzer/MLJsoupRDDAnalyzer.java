package baozi.webcrawler.onspark.common.analyzer;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import baozi.webcralwer.common.utils.LogManager;
import baozi.webcrawler.common.metainfo.BaseURL;
import baozi.webcrawler.common.metainfo.JsoupDocWebPage;
import baozi.webcrawler.common.webcomm.JsoupWebCommManager;
import baozi.webcrawler.onspark.common.entry.OnSparkInstanceFactory;

import com.clearspring.analytics.util.Lists;

public class MLJsoupRDDAnalyzer implements Serializable {
  private static transient LogManager logger = new LogManager(
      MLJsoupRDDAnalyzer.class);
  private Map<String, Integer> docToClass;
  private Map<String, List<String>> docToTerms;
  private Map<String, Double> termToIdf;
  private JavaRDD<LabeledPoint> listOfLabledPoint;
  private transient NaiveBayesModel naiveBayesModel;

  public void train() {
    loadTrainingDataFile("/Users/yliu/mavenWorkspace/eclipse-workspace/OnSparkWebCrawler/conf/training_data.json");
    logger.logDebug("training urls: " + docToClass.toString());
    JavaPairRDD<String, List<String>> docToTermsRDD = fetchDocTerms(docToClass).cache();
    docToTerms = docToTermsRDD.collectAsMap();
    List<Tuple2<String, List<String>>> docToTermsList = docToTermsRDD.collect();
    logger.logDebug("docToTermsList: " + docToTermsList.toString());
    termToIdf = calculateIDF(docToClass, docToTermsList);
    logger.logDebug("termToIdf: " + termToIdf.toString());
    listOfLabledPoint = calculateLabeledPoints(docToClass, docToTermsList,
        termToIdf);
    naiveBayesModel = NaiveBayes.train(listOfLabledPoint.rdd());
  }

  public void predict() {
    Map<String, Integer> docToClass = new HashMap<String, Integer>();
    docToClass.put("http://www.mitbbs.com/article_t/JobHunting/33013183.html",
        -1);// -1 not important. just to use the map
    JavaPairRDD<String, List<String>> docToTermsRDD = fetchDocTerms(docToClass).cache();
    Map<String, List<String>> docToTerms = docToTermsRDD.collectAsMap();
    List<Tuple2<String, List<String>>> docToTermsList = docToTermsRDD.collect();
    Map<String, Double> termToIdf = calculateIDF(docToClass, docToTermsList);
    JavaRDD<LabeledPoint> listOfLabledPoint = calculateLabeledPoints(docToClass, docToTermsList,
        termToIdf);
    LabeledPoint lp = listOfLabledPoint.collect().get(0);
    double predict_result = naiveBayesModel.predict(lp.features());
    logger.logDebug("result is: " + predict_result);
  }

  private JavaRDD<LabeledPoint> calculateLabeledPoints(
      Map<String, Integer> docToClass, List<Tuple2<String, List<String>>> docToTerms,
      Map<String, Double> termToIdf) {
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
      public LabeledPoint call(Tuple2<String, List<String>> input)
          throws Exception {
        // iterate through all terms, check if seen, if not, for each calculate
        // occurrence times
        LinkedHashMap<String, Integer> seen = new LinkedHashMap<>();
        for (String itr : input._2) {
          if (seen.containsKey(itr)) {
            seen.put(itr, seen.get(itr) + 1);
          } else {
            seen.put(itr, 1);
          }
        }
        LinkedList<Tuple2<Integer, Double>> linkedlist = new LinkedList<>();
        int i = 0;
        for (Entry<String, Integer> itr : seen.entrySet()) {
          linkedlist.add(new Tuple2<Integer, Double>(i, (double) (termToIdf.get(itr
              .getKey()) * itr.getValue() / seen.size())));
          i++;
        }
        String key = input._1;
        String key_str = String.valueOf(docToClass.get(key));
        int label = Integer.parseInt(key_str);
        logger.logDebug("label: " + key_str + " ; vec: " + linkedlist.toString() + " ; size: " + linkedlist.size());
        Vector vec = Vectors.sparse(linkedlist.size(), linkedlist);
        return new LabeledPoint(label, vec);
      }
    };
  }

  private Map<String, Double> calculateIDF(Map<String, Integer> docToClass,
      List<Tuple2<String, List<String>>> docToTerms) {
    JavaRDD<Tuple2<String, List<String>>> docToTermRdd = OnSparkInstanceFactory
        .getSparkContext().parallelize(docToTerms);

    logger.logDebug("intermediate0: " + docToTermRdd.collect().toString());

    JavaPairRDD<String, String> imm1 = docToTermRdd.flatMapToPair(
        termListsToRddOfTerms())
        // just to convert entry<doc, terms it contains> to a list of <doc, term> for all terms it contains
        .cache();

    logger.logDebug("intermediate1: " + imm1.collectAsMap().toString());

    JavaPairRDD<String, Iterable<Tuple2<String, String>>> imm2 = imm1
        .groupBy(tuple -> tuple._2).distinct().cache();

    logger.logDebug("intermediate2: " + imm2.collectAsMap().toString());

    // group by term and convert the list of <doc, term> to <term, iterable
    // of docs containing the term>
    // make sure to distinct() because same term multi time is allowed
    return imm2.mapToPair(
        entry -> new Tuple2<String, Double>(entry._1, (double) (docToClass
            .size() / Lists.newArrayList(entry._2).size()))).collectAsMap();
    // convert <term, iterable of docs containing the term> into <term, total num of docs/iterable.size()>
  }

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
        // TODO need tokenization here
        LinkedList<String> charArray = new LinkedList<>();
        Set<Character> dontwantthem = new HashSet<>(Arrays.asList('<', '>',
            '(', ')', ',', '?', '/', ';'));
        for (int i = 0; i < sb.length(); i++) {
          // logger.logDebug(String.valueOf(sb.charAt(i)));
          if (!dontwantthem.contains(sb.charAt(i))) {
            charArray.add(String.valueOf(sb.charAt(i)));
          }
        }
        logger.logDebug("done: " + charArray);

        // TODO need to make sure each char in charArray appears same times as
        // it appears in the doc
        // TODO need to make sure each char is in order as it appears in the doc

        return new Tuple2<String, List<String>>(docUrl, charArray);
      }
    };
  }
}
