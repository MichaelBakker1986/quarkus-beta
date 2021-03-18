package nl.appmodel.realtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import java.nio.file.Path;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class NPMProcess {
    public static final String node_path = System.getenv("NODE_PATH");
    String path;
    String command;
    @SneakyThrows
    public int start() {
        var pb = new ProcessBuilder("npm.cmd", "run", this.command);
        pb.directory(Path.of(path).toFile());
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        return p.waitFor();
    }
}
