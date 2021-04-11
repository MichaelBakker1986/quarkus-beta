package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.HibernateUtill;
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
public class PornHubUpdates implements Update {
    private static final Logger       LOG           = Logger.getLogger(String.valueOf(PornHubUpdates.class));
    private              long         nBytesOffset  = 0;
    private              int          changes       = 0;
    private              long         update_time   = new Date().getTime();
    private final        List<String> sqlStatements = new ArrayList<>();
    private              long         totalLength   = 0;
    private              URL          url;
    private static final char         separator     = '|';
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new PornHubUpdates();
        pornHubUpdates.url = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
        //   var url = new File("C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip").toURI().toURL();
        pornHubUpdates.session = HibernateUtill.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.update_time = new Date().getTime();
        pornHubUpdates.sqlStatements.clear();
        pornHubUpdates.preflight();
        pornHubUpdates.session.getTransaction().commit();
        pornHubUpdates.session.close();
    }
    @Scheduled(cron = "0 56 23 * * ?", identity = "new-pornhub-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        url = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value from prosite.cursors c where c.name='pornhub_videos_file_last_modified'),0)")
                       .getSingleResult()));

        var connection      = (HttpURLConnection) url.openConnection();
        var headerFieldSize = connection.getHeaderField("content-length");
        var lastModified    = connection.getHeaderField("last-modified");
        var content_length  = Long.parseLong(connection.getHeaderField("content-length"));
        long headerModifiedUTC = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                                              .toEpochMilli();
        connection.getInputStream().close();
        if (headerModifiedUTC != cachedLastModified) {
            pornhubVideosOffset(content_length, this::pornhubVideos);
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES ('pornhub_videos_file_last_modified',:pornhub_videos_file_last_modified)")
                   .setParameter(
                           "pornhub_videos_file_last_modified", String.valueOf(headerModifiedUTC)).executeUpdate();
        } else {
            notifier.displayTray("Success - Pornhub - updates", "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
    public void pornhubVideosOffset(long content_length, Runnable runnable) {
        try {
            update_time = new Date().getTime();
            sqlStatements.clear();
            changes = 0;
            var cursor_name = getClass().getSimpleName().toLowerCase() + "_cursor";
            nBytesOffset = Long.parseLong(String.valueOf(
                    session.createNativeQuery(
                            "SELECT IFNULL((SELECT value from prosite.cursors c where c.name=:cursor_name),10930072417)")
                           .setParameter("cursor_name", cursor_name)
                           .getSingleResult()));
            runnable.run();
            batchPersist();
            if (totalLength > nBytesOffset)
                session.createNativeQuery("REPLACE INTO prosite.cursors VALUES (:cursor_name,:totalLength)")
                       .setParameter("totalLength", String.valueOf(totalLength))
                       .setParameter("cursor_name", cursor_name)
                       .executeUpdate();
            notifier.displayTray("Success - " + getClass().getSimpleName(),
                                 "changes [" + changes + "] offset [" + nBytesOffset + "] total [" + totalLength + "]",
                                 MessageType.INFO);
        } catch (Exception e) {
            log.error("ERROR", e);
            notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
        } finally {
            sqlStatements.clear();
            changes = 0;
        }
    }
    @SneakyThrows
    public void pornhubVideos() {
        try (var bis = new BufferedInputStream(url.openStream());
             var zis = new ZipInputStream(bis)) {
            var ze = zis.getNextEntry();
            log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
            var skipped = 0L;
            if (ze.getSize() > nBytesOffset) {
                totalLength = ze.getSize();
                long safe_offset = Math.max(0, nBytesOffset - 1000); //
                //how can JAVA write such bad API, skip (long) is chopped by INTEGER.MAX_VALUE. (why not just take an INTEGER instead..)
                while ((skipped += zis.skip(safe_offset - skipped)) < safe_offset) ;
                var remainder  = new String(zis.readAllBytes());
                var first_lb   = remainder.indexOf('\n');
                var usefulPart = remainder.substring(first_lb + 1);
                try (var read = new StringReader(usefulPart)) {
                    readSourceFile(separator, read, this::readPornhubSourceFileEntry);
                }
            } else {
                log.info("No new data found");
            }
        }
    }
    @SneakyThrows
    private void readPornhubSourceFileEntry(String[] strings) {
        val picture_d  = escape(strings[1]);
        val preview_d  = escape(strings[2]);
        val pornhub_id = picture_d.split("/")[6];
        val header     = escape(strings[3]);
        val tags       = escape(strings[4]);
        val cat        = escape(strings[5]);
        val duration   = sqlNumber(strings[7]);
        val views      = sqlNumber(strings[8]);
        val up         = sqlNumber(strings[9]);
        val down       = sqlNumber(strings[10]);
        var dims       = dims(strings[0]);
        val keyId      = escape(dims.getSrc().split("/")[4]);
        if (!isNumeric(pornhub_id)) {
            log.info("Not updating: [{}]", String.join(" ", strings));
        } else {
            sqlStatements.add(
                    "(" + up + "," + down + "," + views + "," + duration + ",'" + cat + "',\"" + tags + "\",\"" + header + "\",\"" + picture_d + "\",\"" + preview_d + "\"," + dims
                            .getW() + "," + dims.getH() + "," + pornhub_id + ",'" + keyId + "'," + update_time + ",1)");
        }
    }
    @SneakyThrows
    private void batchPersist() {
        if (sqlStatements.isEmpty()) return;
        var sql = """
                  INSERT INTO prosite.pornhub (up,down,views,duration,cat,tag,header,picture_d,preview_d,w,h,pornhub_id,keyid,updated,status) VALUES
                  %s  
                  AS new ON DUPLICATE KEY UPDATE up=new.up,down=new.down,views=new.views, duration=new.duration, cat=new.cat, tag=new.tag, header=new.header, picture_d=new.picture_d, preview_d=new.preview_d, w=new.w, h=new.h, pornhub_id=new.pornhub_id, updated=new.updated, 
                  prosite.pornhub.status=IF(prosite.pornhub.preview_d != new.preview_d,5,  IF (prosite.pornhub.tag != new.tag OR prosite.pornhub.cat != new.cat OR prosite.pornhub.views != new.views,IF(prosite.pornhub.status=2 OR prosite.pornhub.status=4,4,prosite.pornhub.status)));
                  """.formatted(String.join(",\n", sqlStatements));
        changes = session.createNativeQuery(sql)
                         .executeUpdate();
    }
}
