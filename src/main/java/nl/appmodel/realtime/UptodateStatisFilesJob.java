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
public class UptodateStatisFilesJob {
    private final        Notifier notifier  = new Notifier();
    private static final String   workspace = System.getenv("PROSITE_WORKSPACE");
    @Inject              Session  s;
    @Scheduled(cron = "0 40 15 * * ?", identity = "update-static-files")
    @Transactional
    public void updateVideosJob() {
        try {
            new NPMProcess(workspace + "\\react-frontend", "build-dev").start();
            notifier.displayTray("Update static files ", "Changes:", MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Update static files", e.getMessage(), MessageType.ERROR);
            throw e;
        }
    }
}
