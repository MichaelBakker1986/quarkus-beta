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
    /*
        45797341|<iframe src='https://www.xtube.com/video-watch/embedded/pdx-peephole-gloryhole-45797341' frameborder=0 width='640' height='480' scrolling=no name='xt_embed_video'></iframe><br /><a href='https://www.xtube.com/video-watch/pdx-peephole-gloryhole-45797341'>pdx peephole gloryhole</a> powered by <a href='https://www.xtube.com'>XTube</a>|https://www.xtube.com/video-watch/pdx-peephole-gloryhole-45797341|https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/12.jpg|https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/1.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/2.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/3.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/4.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/5.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/6.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/7.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/8.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/9.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/10.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/11.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/12.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/13.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/14.jpg,https://cdn1-s-hw-e5.xtube.com/videos/202102/22/45797341/xtube_preview/15.jpg|pdx peephole gloryhole|barebacked,ebony bbc,glory hole,huge cock anal,pdx|Anal,Bareback,Big Cock|Muscletight1|user_video|gay|11
    */
    public static void main(String[] args) {

        //LegacyUtil.session()
    }
    private static final Logger LOG = Logger.getLogger(String.valueOf(XTubeDownloader.class));
    /*  @SneakyThrows
     private void getRecentDeletes() {
         URL         zipUrl  = new URL("https://www.xtube.com/webmaster/api");
         File        zipFile = new File(zipUrl.toURI());
         ZipFile     zip     = new ZipFile(zipFile);
         InputStream is      = zip.getInputStream(zip.getEntry("Xtube-Recent-Deleted-Videos1.csv"));
     }*/
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
        var    lineaction = new lineaction();
        String inputLine;
        while ((inputLine = in.readLine()) != null)
            lineaction.parse(inputLine);
        in.close();

        LOG.info(newlyDeleted.size() + " records to be deleted");
        //   extracted(newlyDeleted);
    }
}
