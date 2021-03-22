package nl.appmodel.xvideos;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Notifier;
import nl.appmodel.realtime.Update;
import javax.enterprise.context.ApplicationScoped;
import java.awt.TrayIcon.MessageType;
@SuppressWarnings("unused")
@Slf4j
@ToString
@ApplicationScoped
public class XVideosDetails implements Update {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 49 02 * * ?", identity = "xvideos-details")
    public void xvideos_details() {
        log.info("Process done" + new NodeJSProcess(workspace + "\\crawler\\xvideos\\xvideo_detail.js").start());
        new Notifier().displayTray("", "XVideosDetails.xvideos_details() done", MessageType.INFO);
    }
}
