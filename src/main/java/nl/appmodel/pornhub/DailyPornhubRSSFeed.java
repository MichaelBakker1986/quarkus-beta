package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class DailyPornhubRSSFeed {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 01 02 * * ?", identity = "daily-pornhub-rss-feed")
    void updatePornHubFromRRS() {
        new NodeJSProcess(workspace + "\\crawler\\pornhub\\RSSFeed_PornHub.js").startAndLog();
    }
    @SneakyThrows
    @Scheduled(cron = "0 10 02 * * ?", identity = "daily-pornhub-rss-feed-detail")
    void updatePornHubFromRRSDetail() {
        new NodeJSProcess(workspace + "\\crawler\\pornhub\\DetailPornHub.js").startAndLog();
    }
    @SneakyThrows
    @Scheduled(cron = "0 49 22 * * ?", identity = "daily-pornhub-rss-daily-crawl-deleted")
    void DailyCrawlDeleted() {
        new NodeJSProcess(workspace + "\\crawler\\pornhub\\DailyCrawl.js").startAndLog();
    }
}
