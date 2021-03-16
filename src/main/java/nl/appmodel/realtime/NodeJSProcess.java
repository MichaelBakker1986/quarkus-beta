package nl.appmodel.realtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class NodeJSProcess {
    String path;
    @SneakyThrows
    public int start() {
        ProcessBuilder pb = new ProcessBuilder("node", this.path);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        return p.waitFor();
    }
}
