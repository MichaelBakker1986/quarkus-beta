package nl.appmodel.xtube;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.Notifier;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class XTubeDownloader {
    @Inject              Session s;
    private static final String  workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 37 5 * * ?", identity = "update_pro_delete-tube-job")
    @Transactional
    void deleteTubeJob2() {
        try {
            var nativeQuery = s.createNativeQuery("""
                                                                                                UPDATE prosite.pro , prosite.xtube 
                                                  SET prosite.xtube.status = 9,
                                                  prosite.pro.status =9,
                                                  prosite.xtube.updated = UNIX_TIMESTAMP()*1000
                                                  WHERE prosite.xtube.pro_id=pro.id AND prosite.xtube.status =0 AND prosite.xtube.deleted =1;
                                                                                                """);
            int i = nativeQuery.executeUpdate();
            new Notifier().displayTray("XTubeDownloader.deleteTubeJob2() done [" + i + "] updates", MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            new Notifier().displayTray("XTubeDownloader.deleteTubeJob2() failed", MessageType.ERROR);
            throw e;
        }
    }
    @SneakyThrows
    @Scheduled(cron = "0 38 4 * * ?", identity = "delete-tube-job")
    void deleteTubeJob() {
        //start downloading all entries file
        //update insert accoringly
        ProcessBuilder pb = new ProcessBuilder("node", workspace + "\\crawler\\xtube\\XTubeDeletedJob.js");
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        var     i = p.waitFor();
        log.info("Process done" + i);
        new Notifier().displayTray("XTubeDownloader.deleteTubeJob() done", MessageType.INFO);
    }
}
