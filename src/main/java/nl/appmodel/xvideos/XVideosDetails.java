package nl.appmodel.xvideos;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Update;
import javax.enterprise.context.ApplicationScoped;
@SuppressWarnings("unused")
@Slf4j
@ToString
@ApplicationScoped
public class XVideosDetails implements Update {
    @SneakyThrows
    @Scheduled(cron = "0 53 02 * * ?", identity = "xvideos-details-job")
    public void xvideos_details() {
        new NodeJSProcess("\\crawler\\xvideos\\xvideo_detail.js").startAndLog();
    }
}
