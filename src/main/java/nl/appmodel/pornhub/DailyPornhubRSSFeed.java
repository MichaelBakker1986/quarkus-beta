package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Notifier;
import java.awt.TrayIcon.MessageType;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class DailyPornhubRSSFeed {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 01 2 * * ?", identity = "daily-pornhub-rss-feed")
    void updatePornHubFromRRS() {
        var status = new NodeJSProcess(workspace + "\\crawler\\RSSFeed_PornHub.js").start();
        log.info("Process done" + status);
        new Notifier().displayTray("RSS - Pornhub - Finished", "DailyPornhubRSSFeed.updatePornHubFromRRS() done",
                                   status == 0 ? MessageType.INFO : MessageType.ERROR);
    }
    @SneakyThrows
    @Scheduled(cron = "0 10 2 * * ?", identity = "daily-pornhub-rss-feed-detail")
    void updatePornHubFromRRSDetail() {
        var status = new NodeJSProcess(workspace + "\\crawler\\DetailPornHub.js").start();
        log.info("Process done" + status);
        new Notifier().displayTray("RSSDetail - Pornhub - Finished", "DailyPornhubRSSFeed.updatePornHubFromRRSDetail() done",
                                   status == 0 ? MessageType.INFO : MessageType.ERROR);
    }
    @SneakyThrows
    @Scheduled(cron = "0 49 22 * * ?", identity = "daily-pornhub-rss-daily-crawl-deleted")
    void DailyCrawlDeleted() {
        var status = new NodeJSProcess(workspace + "\\crawler\\DailyCrawl.js").start();
        log.info("Process done" + status);
        new Notifier().displayTray("DailyCrawl - Pornhub - Finished", "DailyPornhubRSSFeed.DailyCrawlDeleted() done",
                                   status == 0 ? MessageType.INFO : MessageType.ERROR);
    }
}
