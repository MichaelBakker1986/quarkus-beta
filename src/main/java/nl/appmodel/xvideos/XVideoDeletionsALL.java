package nl.appmodel.xvideos;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.HibernateUtil;
import nl.appmodel.realtime.Update;
import org.hibernate.Session;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
import java.io.BufferedInputStream;
import java.io.StringReader;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipInputStream;
@Slf4j
@ApplicationScoped
public class XVideoDeletionsALL implements Update {
    private static final Logger       LOG           = Logger.getLogger(String.valueOf(XVideoDeletionsALL.class));
    private final static Long         MILLS_IN_DAY  = 86400000L;
    private              int          changes       = 0;
    private              long         update_time   = new Date().getTime();
    private              List<String> sqlStatements = new ArrayList<>();
    private              long         totalLength   = 0;
    private              URL          url;
    private static final String       zip_url       = "https://webmaster-tools.xvideos.com/xvideos.com-deleted-full.csv.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new XVideoDeletionsALL();
        pornHubUpdates.url     = new URL(zip_url);
        pornHubUpdates.session = HibernateUtil.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.update_time = new Date().getTime();
        pornHubUpdates.preflight();
        pornHubUpdates.session.getTransaction().commit();
        pornHubUpdates.session.close();
    }
    @Scheduled(cron = "0 44 03 * * ?", identity = "deletions-all-xvideos-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        preflight(session, this::xvideosVideos, url = new URL(zip_url));
    }
    public void xvideosVideos(long content_length) {
        try {
            update_time = new Date().getTime();
            changes     = 0;
            xvideosDeletedAll();
            batchPersist();
            notifier.displayTray("Success - XVideos - delete", "deleted [" + changes + "] offset [" + 0 + "] total [" + totalLength + "]",
                                 MessageType.INFO);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("ERROR", e);
            notifier.displayTray("Fail - XVideos - delete", e.getMessage(), MessageType.ERROR);
        } finally {
            changes = 0;
            notifier.displayTray("Success - XVideos - delete", "deleted [" + changes + "] offset [" + 0 + "] total [" + totalLength + "]",
                                 MessageType.INFO);
        }
    }
    @SneakyThrows
    private void xvideosDeletedAll() {
        try (var bis = new BufferedInputStream(url.openStream());
             var zis = new ZipInputStream(bis)) {
            //we will only use the first entry
            var ze = zis.getNextEntry();
            //sure this will be only one file..
            log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
            totalLength = ze.getSize();
            var usefulPart = new String(zis.readAllBytes());
            readSourceFile('|', new StringReader(usefulPart), this::readXVideosSourceFileEntry);
        }
    }
    /*
    https://www.xvideos.com/video61751651/catgirl_lover_part_2_-_getting_in_on_with_the_teacher_and_our_personal_catgirl;
    Catgirl Lover Part 2 - Getting in on with the Teacher and our personal catgirl;
    4316 sec;
    http://img-hw.xvideos-cdn.com/videos/thumbs169ll/cb/02/f9/cb02f995a73aedce3edf4b55ba577418/cb02f995a73aedce3edf4b55ba577418.15.jpg;
    <iframe src="https://www.xvideos.com/embedframe/61751651" frameborder=0 width=510 height=400 scrolling=no allowfullscreen=allowfullscreen></iframe>;
    pussy,tits,boobs,naked,stripping,furry,neko;
    ;
    61751651;
    Unknown
     */
    @SneakyThrows
    private void readXVideosSourceFileEntry(String[] strings) {
        val url      = escape(strings[1]);
        var embed_id = url.split("/")[3].replaceAll("[^0-9]", "");
        sqlStatements.add(embed_id);
    }
    @SneakyThrows
    private void batchPersist() {
        if (sqlStatements.isEmpty()) return;
        val new_sqlStatements = sqlStatements;
        sqlStatements = new ArrayList<>();
        val asSet   = new HashSet<>(new_sqlStatements);
        var updated = -new Date().getTime();
        while (!asSet.isEmpty()) {
            var allStatements = new ArrayList<>(asSet);
            var subset        = new ArrayList<>(allStatements.subList(0, Math.min(allStatements.size(), 100000)));
            asSet.removeAll(subset);
            if (!subset.isEmpty()) {
                var sql = """
                          UPDATE prosite.xvideos SET
                          status = 9,
                          updated = %s
                          WHERE embed_id IN (%s)
                          """.formatted(updated, String.join(",", subset));
                changes += session.createNativeQuery(sql)
                                  .executeUpdate();
            }
        }
        var pro_sql_update = """
                             UPDATE prosite.pro p
                             INNER JOIN prosite.xvideos x ON x.pro_id = p.id  
                             SET p.status = 9,
                                 p.updated =  :updated
                             WHERE x.updated = :updated
                             """;
        changes += session.createNativeQuery(pro_sql_update)
                          .setParameter("updated", updated)
                          .executeUpdate();
    }
}
