package nl.appmodel.youporn;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import javax.enterprise.context.ApplicationScoped;
@Slf4j
@ToString
@ApplicationScoped
public class DailyYouPornRSSFeed {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 30 23 * * ?", identity = "daily-youporn-rss-daily-crawl-deleted")
    public void DailyCrawlDeleted() {
      new NodeJSProcess(workspace + "\\crawler\\youporn\\DailyCrawlYouporn.js").startAndLog();
    }
}
