package baozi.webcrawler.onspark.common.entry;

import baozi.webcrawler.onspark.common.workflow.OnSparkWorkflowManager;

class Kicker {

  public void kick() {
    ConfigLoader configLoader = new ConfigLoader();
    configLoader.load();
    OnSparkWorkflowManager workflow = new OnSparkWorkflowManager();
    workflow.crawl();
  }
}
