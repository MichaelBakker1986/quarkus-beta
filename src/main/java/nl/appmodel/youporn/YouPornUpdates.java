package nl.appmodel.youporn;

import com.google.common.base.Joiner;
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
import java.io.File;
import java.io.StringReader;
import java.net.URL;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipInputStream;
@Slf4j
@ApplicationScoped
public class YouPornUpdates implements Update {
    private static final Logger       LOG           = Logger.getLogger(String.valueOf(YouPornUpdates.class));
    private              long         nBytesOffset  = 0;
    private              int          changes       = 0;
    private              long         update_time   = new Date().getTime();
    private final        List<String> sqlStatements = new ArrayList<>();
    private static final char         separator     = '|';
    private static final String       zip_url       = "https://www.youporn.com/YouPorn-Embed-Videos-Dump.zip";
    private              URL          url;
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new YouPornUpdates();
        pornHubUpdates.url = new File("C:\\Users\\michael\\Documents\\Downloads\\YouPorn-Embed-Videos-Dump.zip").toURI().toURL();
        //pornHubUpdates.url     = new URL(zip_url);
        pornHubUpdates.session = HibernateUtil.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.update_time = new Date().getTime();
        pornHubUpdates.sqlStatements.clear();
        pornHubUpdates.download(0l);
        pornHubUpdates.session.getTransaction().commit();
        pornHubUpdates.session.close();
    }
    @Scheduled(cron = "0 56 23 * * ?", identity = "new-youporn-videos")
    @Transactional
    @SneakyThrows
    public final void preflight() {
        url = new URL(zip_url);
        this.preflight(session, this::download, url);
    }
    public void download(long content_length) {
        try {
            update_time = new Date().getTime();
            sqlStatements.clear();
            changes      = 0;
            nBytesOffset = Long.parseLong(String.valueOf(
                    session.createNativeQuery(
                            "SELECT IFNULL((SELECT value from prosite.cursors c where c.name=:name),:default_offset)")
                           .setParameter("name", getClass().getSimpleName().toLowerCase() + "_file_cursor")
                           .setParameter("default_offset", 1211164259 - (1024 * 100))
                           .getSingleResult()));
            long totalLength = zipUpdate(separator);
            batchPersist();
            if (totalLength > nBytesOffset)
                session.createNativeQuery("REPLACE INTO prosite.cursors VALUES (:name,:totalLength)")
                       .setParameter("name", getClass().getSimpleName().toLowerCase() + "_file_cursor")
                       .setParameter("totalLength", String.valueOf(totalLength))
                       .executeUpdate();
            notifier.displayTray("Success - " + getClass().getSimpleName(),
                                 "changes [" + changes + "] offset [" + nBytesOffset + "] total [" + totalLength + "]",
                                 MessageType.INFO);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("ERROR", e);
            notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
        } finally {
            sqlStatements.clear();
            changes = 0;
        }
    }
    @SneakyThrows
    public long zipUpdate(char sep) {
        var totalLength = 0L;
        try (var bis = new BufferedInputStream(url.openStream());
             var zis = new ZipInputStream(bis)) {
            //we will only use the first entry
            var ze = zis.getNextEntry();
            //sure this will be only one file..
            log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
            var skipped = 0L;
            if (ze.getSize() > nBytesOffset) {
                totalLength = ze.getSize();
                long safe_offset = Math.min(ze.getSize(), Math.max(0L, nBytesOffset - 1000L)); //
                //how can JAVA write such bad API, skip (long) is chopped by INTEGER.MAX_VALUE. (why not just take an INTEGER instead..)
                if (safe_offset > 0)
                    while ((skipped += zis.skip(safe_offset - skipped)) < safe_offset) ;
                var remainder  = new String(zis.readAllBytes());
                var first_lb   = remainder.indexOf('\n');
                var usefulPart = remainder.substring(first_lb + 1);
                readSourceFile(sep, new StringReader(usefulPart), this::parseEntry);
            } else {
                log.info("No new data found");
            }
        }
        return totalLength;
    }
    @SneakyThrows
    public void parseEntry(String[] strings) {
        try {
            var iframe      = strings[0].substring(0, strings[0].indexOf("/iframe") + 8);
            val code        = escape(iframe);
            val picture_m   = escape(strings[1]);
            val header      = escape(strings[2]);
            val tags        = escape(strings[3]);
            val cat         = escape(strings[4]);
            val duration_ui = escape(strings[6]);
            int duration    = Integer.parseInt(strings[7]);
            val url         = escape(strings[8]);
            val youporn_id  = Long.parseLong(strings[9]);

            var dims  = dims(iframe);
            var actor = "";
            if (!strings[5].isEmpty()) {
                actor = escape(strings[5]);
            }
            sqlStatements.add(
                    "(\"" + code + "\",\"" + duration_ui + "\"," + duration + ",\"" + cat + "\",\"" + tags + "\",\"" + header + "\",\"" + picture_m + "\"," + dims
                            .getW() + "," + dims.getH() + "," + youporn_id + ",\"" + actor + "\",\"" + url + "\"," + update_time + ",1)");
        } catch (Exception e) {
            log.warn("Failed to parse [{}] because [{}]", Joiner.on("\n").join(strings), e.getMessage());
        }
    }
    @SneakyThrows
    private void batchPersist() {
        if (sqlStatements.isEmpty()) return;
        var sql = """
                  INSERT INTO prosite.youporn (code,duration_ui,duration,cat,tag,header,picture_m,w,h,youporn_id,actor,url,updated,status) VALUES
                  %s 
                  AS new ON DUPLICATE KEY UPDATE code=new.code, duration_ui=new.duration_ui, duration=new.duration, cat=new.cat, tag=new.tag, header=new.header, picture_m=new.picture_m, w=new.w, h=new.h, youporn_id=new.youporn_id, actor=new.actor, url=new.url, updated=new.updated, prosite.youporn.status=IF(prosite.youporn.status=2,2,1);
                  """.formatted(String.join(",\n", sqlStatements));
        changes = session.createNativeQuery(sql)
                         .executeUpdate();
    }
}
