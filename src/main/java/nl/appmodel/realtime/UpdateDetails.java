package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.Network;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@Slf4j
public class UpdateDetails {
    private final Notifier notifier = new Notifier();
    @Inject       Session  session;
    @SneakyThrows
    public static void main(String[] args) {
        var updateDetails = new UpdateDetails();
        HibernateUtil.run(updateDetails::updateEntities);
    }
    /**
     * update details per network
     */
    @SuppressWarnings("SqlResolve")
    @Transactional
    @Scheduled(cron = "0 00 21 * * ?", identity = "update-entities-from-networks")
    public void updateEntities() {
        var updated       = System.currentTimeMillis();
        var total_changes = 0;
        for (Network network : Network.values()) {

            val changes = session.createNativeQuery("""
                                                    INSERT INTO prosite.pro (id, thumbs, downloaded, views, tag_set, header, embed, w, h, status, duration,ref,updated) 
                                                    SELECT pro_id,%s,n.status=2,IFNULL(n.views,-1),%s,header,%s,%s,%s,1,IFNULL(n.duration,-1),null,:updated From %s n where n.pro_id IS NOT NULL AND n.status=1
                                                    ON DUPLICATE KEY UPDATE
                                                                          thumbs=n.%s,
                                                                          views=IFNULL(n.views,-1),
                                                                          tag_set=%s,
                                                                          header=n.header,
                                                                          embed=%s,
                                                                          w=%s,
                                                                          h=%s,
                                                                          duration=IFNULL(n.duration,-1),
                                                                          ref=null,
                                                                          updated=:updated
                                                    """.formatted(network.getThumb_col(), network.getTagSetJoiner(), network.getCode(),
                                                                  network.getW(), network.getH(),
                                                                  network.tableName(),
                                                                  network.getThumb_col(),
                                                                  network.getTagSetJoinerNew(),
                                                                  network.getCode_new(),
                                                                  network.getW_new(),
                                                                  network.getH_new()
                                                                 )
                                                   )
                                 .setParameter("updated", updated)
                                 .executeUpdate();
            val pro_id_changes = session.createNativeQuery("""
                                                           UPDATE %s x, prosite.pro p
                                                           set x.status = IF(p.downloaded,2,3), 
                                                               p.status = IF(p.downloaded,4,3),
                                                               x.updated = :updated  
                                                           WHERE p.id=x.pro_id 
                                                                 AND x.status = 1 
                                                           """.formatted(network.tableName())
                                                          ).setParameter("updated", updated)
                                        .executeUpdate();
            total_changes += (changes + pro_id_changes);
            log.info("Network: [{}] changes [{}] [{}] in timestamp:[{}]", network.name(), changes, pro_id_changes, updated);
        }

        log.info("Done with timestamp:[{}] changes:[{}]", updated, total_changes);
        notifier.displayTray("Updating -details pro", "Updating pro details", MessageType.INFO);
    }
    public void updateEntities(Session session) {
        this.session = session;
        this.updateEntities();
    }
}
