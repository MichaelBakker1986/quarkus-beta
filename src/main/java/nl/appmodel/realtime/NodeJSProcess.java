package nl.appmodel.realtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import java.awt.TrayIcon.MessageType;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class NodeJSProcess {
    private static final String   workspace = System.getenv("PROSITE_WORKSPACE");
    private static final Notifier notifier  = new Notifier();
    String path;
    @SneakyThrows
    public int start() {
        ProcessBuilder pb = new ProcessBuilder("node", workspace + this.path);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        return p.waitFor();
    }
    public void startAndLog() {
        try {
            var status = this.start();
            log.info("Process [{}] done [{}]", this.path, status);
            notifier.displayTray("Finished", this.path + " done.", status == 0 ? MessageType.INFO : MessageType.ERROR);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Failed " + this.path, e.getMessage(), MessageType.ERROR);
        }
    }
}
