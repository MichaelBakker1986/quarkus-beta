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
                                              drop table if exists tmp_host;
                                              create table tmp_host($domain varchar(72),status int,min int,max int,count int) ENGINE=MEMORY SELECT $domain,status,min(id) min,max(id) max,count(*) count FROM prosite.pro_info 
                                              where $valid_domain =1 
                                              group by $domain,status;
                                                                                            
                                              REPLACE INTO prosite.host 
                                              (SELECT th.$domain domain,IFNULL(IFNULL(p.pointer,dl.dl),th.min) pointer,th.min start,th.max end, IFNULL(d.downloaded,0) downloaded,IFNULL(e.err,0) errors,IFNULL(dl.dl,0) dl, %s updated FROM 
                                              (SELECT $domain,min(min) min,max(max) max FROM tmp_host GROUP BY $domain) as th
                                              LEFT OUTER JOIN (SELECT $domain, MIN(min) pointer FROM tmp_host WHERE status = 3 GROUP BY $domain) as p ON th.$domain =p.$domain
                                              LEFT OUTER JOIN (SELECT $domain, MAX(max) dl FROM tmp_host WHERE status = 2 GROUP BY $domain) as dl ON th.$domain =dl.$domain
                                              LEFT OUTER JOIN (SELECT $domain, SUM(count) downloaded FROM tmp_host WHERE status = 2 GROUP BY $domain) as d ON th.$domain =d.$domain
                                              LEFT OUTER JOIN (SELECT $domain, SUM(count) err FROM tmp_host WHERE status >= 9 GROUP BY $domain) as e ON th.$domain =e.$domain
                                              );
                                              drop table if exists tmp_host;
                                              """.formatted(System.currentTimeMillis()))
                           .executeUpdate();
            notifier.displayTray("Update host table", "Changes: " + changes, MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Update host table", e.getMessage(), MessageType.ERROR);
            throw e;
        }
    }
}
