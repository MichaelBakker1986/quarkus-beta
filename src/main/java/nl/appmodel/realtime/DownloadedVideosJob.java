package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@Slf4j
@ToString
public class DownloadedVideosJob {
    private final Notifier notifier = new Notifier();
    @Inject       Session  s;
    @Scheduled(cron = "0 33 20 * * ?", identity = "updateVideosJob")
    @Transactional
    public void updateVideosJob() {
        try {
            int changes = s.createNativeQuery("""
                                              INSERT INTO prosite.host
                                              SELECT h.*,prosite.currentmillis() updated from (SELECT 
                                               SUBSTRING_INDEX(SUBSTRING_INDEX(thumbs, '/', 3), '://', -1) name,
                                               min(id) counter,
                                               min(id) start,
                                               max(id) end,
                                               count(*) downloaded
                                              from prosite.pro
                                              where status = 2
                                              group by SUBSTRING_INDEX(SUBSTRING_INDEX(thumbs, '/', 3), '://', -1) )as h
                                               ON DUPLICATE KEY UPDATE 
                                                downloaded=h.downloaded,
                                                start=h.start,
                                                end=h.end,
                                                updated=prosite.currentmillis(); 
                                              """)
                           .executeUpdate();
            notifier.displayTray("Update host table", "Changes: " + changes, MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Update host table", e.getMessage(), MessageType.ERROR);
            throw e;
        }
    }
}
