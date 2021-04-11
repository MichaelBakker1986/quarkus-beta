package nl.appmodel.xtube;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.realtime.NodeJSProcess;
import nl.appmodel.realtime.Update;
import org.hibernate.Session;
import javax.inject.Inject;
@SuppressWarnings("unused")
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class XTubeDownloader implements Update {
    @Inject Session s;
    @SneakyThrows
    @Scheduled(cron = "0 38 4 * * ?", identity = "delete-tube-job")
    void deleteTubeJob() {
        new NodeJSProcess("\\crawler\\xtube\\XTubeDeletedJob.js").startAndLog();
    }
    @SneakyThrows
    @Scheduled(cron = "0 00 9 * * ?", identity = "arrivals-xtube-job")
    void arrivalsAddJob() {
        new NodeJSProcess("\\crawler\\xtube\\XTubeArrivalsJob.js").startAndLog();
    }
}
