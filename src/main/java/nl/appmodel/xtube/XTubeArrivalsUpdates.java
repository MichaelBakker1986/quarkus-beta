package nl.appmodel.xtube;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import nl.appmodel.LegacyUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class XTubeArrivalsUpdates {

    private static final Logger LOG = Logger.getLogger(String.valueOf(XTubeDownloader.class));

    @SneakyThrows
    public void goDelete() {
        LOG.info("job start");
        val url = new URL(
                "https://cdn1-s-hw-e1.xtube.com/videowall/webmaster/Xtube-Recent-Deleted-Videos-Feed.zip?cb=b663e39543f01aaff06d55d5f03c2738");
        var         url1    = new File("H:\\download\\deleted.csv").toURI().toURL();
        var         s       = LegacyUtil.session();
        Set<String> deleted = new HashSet<String>(s.createNativeQuery("select xtube_id from xtube where deleted=1;").list());
        LOG.info("Got deleted ids");
        Set<String> available = new HashSet<String>(s.createNativeQuery("select xtube_id from xtube where deleted=0;").list());
        LOG.info("Got cache from database deleted: [" + deleted.size() + "] avail: [" + available.size() + "]");
        var newlyDeleted = new HashSet<String>();

        val in = new BufferedReader(new InputStreamReader(url.openStream()));

        class lineaction {
            int last = 0;
            void parse(String line) {
                var next       = line.split(",");
                var pornhub_id = next[0].trim();
                if ((last++ % 1000) == 0)
                    LOG.info("@" + last);
                if (!deleted.contains(pornhub_id) && available.contains(pornhub_id)) {
                    deleted.add(pornhub_id);
                    newlyDeleted.add(pornhub_id);
                    LOG.info("new deleted record " + pornhub_id);
                }
            }
        }
        var    lineAction = new lineaction();
        String inputLine;
        while ((inputLine = in.readLine()) != null)
            lineAction.parse(inputLine);
        in.close();

        LOG.info(newlyDeleted.size() + " records to be deleted");
    }
}
