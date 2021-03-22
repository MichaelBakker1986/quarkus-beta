package nl.appmodel.xvideos;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Notifier;
import nl.appmodel.realtime.Update;
import java.awt.TrayIcon.MessageType;
@SuppressWarnings("unused")
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class XVideosDetails implements Update {
    private static final String workspace = System.getenv("PROSITE_WORKSPACE");
    @SneakyThrows
    @Scheduled(cron = "0 45 2 * * ?", identity = "xvideos-details")
    void xvideos_details() {
        log.info("Process done" + new NodeJSProcess(workspace + "\\crawler\\xvideos\\xvideo_detail.js").start());
        new Notifier().displayTray("", "XVideosDetails.xvideos_details() done", MessageType.INFO);
    }
}
