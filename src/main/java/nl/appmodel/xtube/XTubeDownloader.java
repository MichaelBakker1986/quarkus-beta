package nl.appmodel.xtube;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Notifier;
import nl.appmodel.realtime.Update;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@SuppressWarnings("unused")
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class XTubeDownloader implements Update {
    @Inject              Session s;
    private static final String  workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 37 5 * * ?", identity = "update_pro_delete-tube-job")
    @Transactional
    void deleteTubeJob2() {
        try {
            var nativeQuery = s.createNativeQuery("""
                                                            UPDATE prosite.pro p, prosite.xtube x, prosite.pro_tags pt,prosite.tags t 
                                                            SET x.status = 9,
                                                            p.status =9,
                                                            x.updated = :updated,
                                                            t.updated = -:updated
                                                            WHERE x.pro_id=p.id 
                                                            AND p.id=pt.pro 
                                                            AND pt.tag=t.id
                                                            AND x.status =0 AND x.deleted =1;
                                                  """);
            int i = nativeQuery.setParameter("updated", System.currentTimeMillis()).executeUpdate();
            new Notifier().displayTray("", "XTubeDownloader.deleteTubeJob2() done [" + i + "] updates", MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            new Notifier().displayTray("", "XTubeDownloader.deleteTubeJob2() failed", MessageType.ERROR);
            throw e;
        }
    }
    @SneakyThrows
    @Scheduled(cron = "0 38 4 * * ?", identity = "delete-tube-job")
    void deleteTubeJob() {
        new NodeJSProcess(workspace + "\\crawler\\xtube\\XTubeDeletedJob.js").startAndLog();
    }
    @SneakyThrows
    @Scheduled(cron = "0 00 9 * * ?", identity = "arrivals-xtube-job")
    void arrivalsAddJob() {
        new NodeJSProcess(workspace + "\\crawler\\xtube\\XTubeArrivalsJob.js").startAndLog();
    }
}
