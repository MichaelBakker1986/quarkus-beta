package nl.appmodel.youporn;

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
public class DailyYouPornRSSFeed {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 30 23 * * ?", identity = "daily-youporn-rss-daily-crawl-deleted")
    void DailyCrawlDeleted() {
        var status = new NodeJSProcess(workspace + "\\crawler\\youporn\\DailyCrawlYouporn.js").start();
        log.info("Process done" + status);
        new Notifier().displayTray("DailyCrawlYouporn - Finished", "DailyPornhubRSSFeed.DailyCrawlYouporn() done",
                                   status == 0 ? MessageType.INFO : MessageType.ERROR);
    }
}
