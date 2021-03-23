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
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipInputStream;
@Slf4j
@ApplicationScoped
public class XVideoUpdates implements Update {
    private static final Logger       LOG           = Logger.getLogger(String.valueOf(XVideoUpdates.class));
    private              int          changes       = 0;
    private              long         update_time   = new Date().getTime();
    private final        List<String> sqlStatements = new ArrayList<>();
    private              long         totalLength   = 0;
    private              URL          url;
    private static final String       zip_url       = "https://webmaster-tools.xvideos.com/xvideos.com-export-week.csv.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new XVideoUpdates();
        pornHubUpdates.url = new URL(zip_url);
        //   var url = new File("C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip").toURI().toURL();
        pornHubUpdates.session = HibernateUtil.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.update_time = new Date().getTime();
        pornHubUpdates.sqlStatements.clear();
        pornHubUpdates.preflight();
        pornHubUpdates.session.getTransaction().commit();
        pornHubUpdates.session.close();
    }
    @Scheduled(cron = "0 00 18 * * ?", identity = "new-xvideos-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        preflight(url = new URL(zip_url));
    }
    @SneakyThrows
    public void preflight(URL url) {
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value from prosite.cursors c where c.name='xvideos_videos_file_last_modified'),0)")
                       .getSingleResult()));

        var connection      = (HttpURLConnection) url.openConnection();
        var headerFieldSize = connection.getHeaderField("content-length");
        var lastModified    = connection.getHeaderField("last-modified");
        long headerModifiedUTC = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                                              .toEpochMilli();
        connection.getInputStream().close();
        if (headerModifiedUTC != cachedLastModified) {
            xvideosVideos();
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES ('xvideos_videos_file_last_modified',:file_last_modified)")
                   .setParameter(
                           "file_last_modified", String.valueOf(headerModifiedUTC)).executeUpdate();
        } else {
            notifier.displayTray("Success - XVideos - updates", "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
    public void xvideosVideos() {
        try {
            update_time = new Date().getTime();
            sqlStatements.clear();
            changes = 0;
            xvideosLastWk();
            batchPersist();
            notifier.displayTray("Success - XVideos", "changes [" + changes + "] offset [" + 0 + "] total [" + totalLength + "]",
                                 MessageType.INFO);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("ERROR", e);
            notifier.displayTray("Fail - XVideos - updates", e.getMessage(), MessageType.ERROR);
        } finally {
            sqlStatements.clear();
            changes = 0;
        }
    }
    @SneakyThrows
    private void xvideosLastWk() {
        try (var bis = new BufferedInputStream(url.openStream());
             var zis = new ZipInputStream(bis)) {
            //we will only use the first entry
            var ze = zis.getNextEntry();
            //sure this will be only one file..
            log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(),
                     LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
            totalLength = ze.getSize();
            var usefulPart = new String(zis.readAllBytes());
            readSourceFile(';', new StringReader(usefulPart), this::readXVideosSourceFileEntry);
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
        val url         = escape(strings[0]);
        val header      = escape(strings[1]);
        val duration_ui = escape(strings[2]);
        val picture_m   = escape(strings[3]);
        val code        = strings[4];
        val tags        = escape(strings[5]);
        val actor       = escape(strings[6]);
        val embed_id    = escape(strings[7]);
        val cat         = escape(strings[8]);
        val dim         = dims(code);
        val duration    = parseInt(duration_ui);
        sqlStatements.add(
                "(\"" + escape(
                        code) + "\",\"" + url + "\",\"" + duration_ui + "\"," + duration + ",\"" + cat + "\",\"" + tags + "\",\"" + header + "\",\"" + picture_m + "\"," + dim
                        .getW() + "," + dim.getH() + ",\"" + actor + "\"," + embed_id + "," + update_time + ",1)");
    }
    @SneakyThrows
    private void batchPersist() {
        if (sqlStatements.isEmpty()) return;
        var sql = """
                  INSERT INTO prosite.xvideos (code,url,duration_ui,duration,cat,tag,header,picture_m,w,h,actor,embed_id,updated,status) VALUES
                  %s 
                  AS new ON DUPLICATE KEY UPDATE code=new.code, url=new.url, duration_ui=new.duration_ui, duration=new.duration, cat=new.cat, tag=new.tag, header=new.header, picture_m=new.picture_m, w=new.w, h=new.h, actor=new.actor, embed_id=new.embed_id, updated=new.updated, prosite.xvideos.status=IF(prosite.xvideos.status=2,2,1);
                  """.formatted(String.join(",\n", sqlStatements));
        changes = session.createNativeQuery(sql)
                         .executeUpdate();
    }
}
