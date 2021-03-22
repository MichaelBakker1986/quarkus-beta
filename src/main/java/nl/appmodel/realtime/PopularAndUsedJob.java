package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@Slf4j
@ToString
public class PopularAndUsedJob {
    private final Notifier notifier = new Notifier();
    @Inject       Session  s;
    @Scheduled(cron = "0 55 15 * * ?", identity = "most-viewed-most-popular-updates")
    @Transactional
    public void mostViewedMostPopular() {
        try {
            /*#MOST_POPULAR*/
            /* #MOST_USED*/
            int changes_popularity = s.createNativeQuery("""
                                                         UPDATE prosite.tags t
                                                            JOIN
                                                               (SELECT 
                                                                   pt.tag, 
                                                                   COUNT(*) used,
                                                                   SUM(p.views) popular        
                                                               FROM
                                                                   prosite.pro_tags pt, prosite.pro p
                                                               WHERE
                                                                   pt.pro = p.id AND p.status = 2
                                                               GROUP BY pt.tag) pt 
                                                               ON pt.tag = t.id
                                                           SET
                                                               t.used = pt.used,
                                                               t.popularity = pt.popular,
                                                               t.updated = :updated
                                                           WHERE t.updated <= 0;
                                                             """
                                                        )
                                      .setParameter("updated", System.currentTimeMillis())
                                      .setHint(
                                              "javax.persistence.lock.timeout",
                                              LockOptions.SKIP_LOCKED
                                              )
                                      .executeUpdate();
            notifier.displayTray("Update MOST view & most popular", "Changes: " + changes_popularity, MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Update MOST view& most popular", e.getMessage(), MessageType.ERROR);
            throw e;
        }
    }
}
