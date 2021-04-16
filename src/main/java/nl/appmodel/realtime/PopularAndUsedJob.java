package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.model.Network;
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
    @SuppressWarnings("SqlResolve")
    @Transactional
    @Scheduled(cron = "0 30 15 * * ?", identity = "remove-entities-from-pro-and-tags")
    public void removeInvalidEntries() {

        val changes = s.createNativeQuery("""
                                          UPDATE tag 
                                          FORCE KEY (state_tag) NATURAL JOIN tag_active_states
                                          JOIN pro_tag USING (tag)
                                          JOIN errors USING (pro)
                                          SET state = 1,updated=DEFAULT;

                                          UPDATE thumb 
                                          FORCE KEY (pro_state) NATURAL JOIN thumb_valid_states
                                          JOIN errors USING (pro)
                                          SET state = 4;
                                                                                          
                                          DELETE p,pt
                                          FROM errors
                                          JOIN pro p USING (pro)
                                          JOIN pro_tag pt USING (pro);
                                          """
                                         )
                       .executeUpdate();
        notifier.displayTray("Removing -details pro", "Removing [ " + changes + "]", MessageType.INFO);
    }
    /**
     * update details per network
     */
    @SuppressWarnings("SqlResolve")
    @Transactional
    @Scheduled(cron = "0 40 15 * * ?", identity = "update-entities-from-networks")
    public void updateEntities() {
        var total_changes = 0;
        val changes = s.createNativeQuery("""
                                          UPDATE pro p
                                             JOIN stats_changes v USING (pro)
                                          SET 
                                              p.views=v.views,
                                              p.up=v.up,
                                              p.down=v.down
                                          WHERE p.crc_stats<>v.crc_stats    
                                          """)
                       .executeUpdate();
        total_changes += (changes);
        log.info(" changes [{}]", changes);
        log.info("Done with changes:[{}]", total_changes);
        notifier.displayTray("Updating -details pro", "Updating pro details", MessageType.INFO);
    }
    @Scheduled(cron = "0 55 15 * * ?", identity = "most-viewed-most-popular-updates")
    @Transactional
    public void mostViewedMostPopular() {
        try {
            int changes_popularity = s.createNativeQuery("""
                                                         UPDATE pro_tag pt
                                                         JOIN stats_changes USING (pro)
                                                         JOIN pro USING (pro)
                                                         SET pt.views = pro.views;
                                                                                                                  
                                                         UPDATE tag t
                                                         FORCE KEY (state_tag) NATURAL JOIN tag_valid_states 
                                                             LEFT OUTER JOIN
                                                                 (SELECT
                                                                    tag,
                                                                    COUNT(*) used,
                                                                    SUM(views) popular,
                                                         		    ROW_NUMBER() OVER (ORDER BY (COUNT(*)) ASC) rank_used,
                                                                    ROW_NUMBER() OVER (ORDER BY SUM(views) ASC) rank_number,      
                                                                    ROW_NUMBER() OVER (ORDER BY (SUM(views)/COUNT(*)) ASC) rank_special
                                                                    FROM pro_tag
                                                                     GROUP BY tag) sort 
                                                                USING (tag)
                                                         SET t.state = 2,
                                                         t.updated = DEFAULT,
                                                         t.popularity =IFNULL(sort .rank_number,0),
                                                         t.special =IFNULL(sort .rank_special,0),
                                                         t.rank_used =IFNULL(sort .rank_used,0),
                                                         t.used =IFNULL(sort .used,0)
                                                         ;
                                                             """
                                                        )
                                      .setHint(
                                              "javax.persistence.lock.timeout",
                                              LockOptions.SKIP_LOCKED
                                              )
                                      .executeUpdate();

            var total_changes = 0;
            for (Network network : Network.values()) {
                @SuppressWarnings("SqlResolve")
                val changes = s.createNativeQuery("""
                                                  UPDATE %n n
                                                  JOIN stats_changes USING (pro)
                                                  JOIN pro p USING (pro)
                                                  SET 
                                                      p.updated=DEFAULT,
                                                      n.updated=DEFAULT,
                                                      p.crc_stats=n.crc_stats,
                                                      n.flag = n.flag | 4  
                                                  """.formatted(network.tableName())
                                                 )
                               .executeUpdate();
                total_changes += (changes);
                log.info("Network: [{}] changes [{}]", network.name(), changes);
            }

            log.info("Done with changes:[{}]", total_changes);
            notifier.displayTray("Updating -details pro", "Updating pro details", MessageType.INFO);

            notifier.displayTray("Update MOST view & most popular", "Changes: " + changes_popularity, MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Update MOST view& most popular", e.getMessage(), MessageType.ERROR);
            throw e;
        }
    }
}
