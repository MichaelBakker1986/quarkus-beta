package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.HibernateUtil;
import nl.appmodel.realtime.Update;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
@Slf4j
@ApplicationScoped
public class FullPornHubUpdates implements Update {
    private static final Logger       LOG           = Logger.getLogger(String.valueOf(FullPornHubUpdates.class));
    private              long         nBytesOffset  = 0;
    private              int          changes       = 0;
    private              long         update_time   = new Date().getTime();
    private final        List<String> sqlStatements = new ArrayList<>();
    private              long         totalLength   = 0;
    private              URL          url;
    private static final char         separator     = '|';
    int total = 0;
    @Inject
    ManagedExecutor executor;
    Executor executor_used;
    @Inject SessionFactory fact;
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new FullPornHubUpdates();
        //pornHubUpdates.url = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
        pornHubUpdates.url           = new File("C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip").toURI().toURL();
        pornHubUpdates.executor_used = Executors.newCachedThreadPool();
        pornHubUpdates.fact          = HibernateUtil.em();
        //  pornHubUpdates.session       = HibernateUtil.getCurrentSession();
        //  pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.update_time = new Date().getTime();
        pornHubUpdates.sqlStatements.clear();
        pornHubUpdates.pornhubVideos();
        pornHubUpdates.batchPersist();
        //     pornHubUpdates.session.getTransaction().commit();
        //   pornHubUpdates.session.close();
    }
    @Scheduled(cron = "0 09 02 * * ?", identity = "full-pornhub-videos-update")
    @Transactional
    @SneakyThrows
    public void preflight() {
        executor_used = executor;
        url           = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
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
            this.pornhubVideos();
            //pornhubVideosOffset(content_length, this::pornhubVideos);
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES ('pornhub_videos_file_last_modified',:pornhub_videos_file_last_modified)")
                   .setParameter(
                           "pornhub_videos_file_last_modified", String.valueOf(headerModifiedUTC)).executeUpdate();
        } else {
            notifier.displayTray("Success - Pornhub - updates", "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
    @SneakyThrows
    public void pornhubVideos() {
        try (var bis = new BufferedInputStream(url.openStream());
             var zis = new ZipInputStream(bis)) {
            ZipEntry ze = zis.getNextEntry();
            log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
            BufferedReader br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
            readSourceFile(separator, br, this::readPornhubSourceFileEntry);
        }
    }
    @SneakyThrows
    private void readPornhubSourceFileEntry(String[] strings) {
        if (strings.length < 13 || Arrays.stream(strings).anyMatch(s -> s == null)) {
            LOG.warning("Invalid record, skipping [" + String.join(" ", strings) + "]");
            return;
        }
        try {

            val picture_d  = escape(strings[11]);
            val preview_d  = escape(strings[12]);
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
            if (sqlStatements.size() == 10000) {
                batchPersist();
                //LOG.info("Changes [" + changes + "][" + sqlStatements.size() + "]");
            }
        } catch (Exception e) {
            log.warn("Invalid record", String.join(" ", strings));
            e.printStackTrace();
        }
    }
    /**
     * batch large request synchronous partitioning
     */
    @SneakyThrows
    private void batchPersist() {
        var rest = sqlStatements;
        while (!rest.isEmpty()) {
            var batch_size = Math.min(100000, rest.size());
            var subSet     = new ArrayList<>(rest.subList(0, batch_size));
            rest = rest.subList(batch_size, rest.size());
            persistBatch(subSet);
        }
        sqlStatements.clear();
    }
    /**
     * Multithreading persisting solution
     */
    @Transactional
    private void persistBatch(List<String> subSet) {
        var block = """
                    INSERT INTO prosite.pornhub (up,down,views,duration,cat,tag,header,picture_d,preview_d,w,h,pornhub_id,keyid,updated,status) VALUES
                    %s  
                    AS new ON DUPLICATE KEY UPDATE 
                    up=new.up,
                    down=new.down,
                    views=new.views, 
                    duration=new.duration, 
                    cat=new.cat, 
                    tag=new.tag, 
                    header=new.header, 
                    picture_d=new.picture_d, 
                    preview_d=new.preview_d, 
                    w=new.w, 
                    h=new.h, 
                    pornhub_id=new.pornhub_id, 
                    updated=new.updated, 
                    prosite.pornhub.status=IF(prosite.pornhub.picture_d != new.picture_d,5,  
                                               IF (prosite.pornhub.tag != new.tag OR prosite.pornhub.cat != new.cat OR prosite.pornhub.views != new.views,
                                                   IF(prosite.pornhub.status=2 OR prosite.pornhub.status=4,4,prosite.pornhub.status),
                                                   prosite.pornhub.status));
                    """.formatted(String.join(",\n", subSet));

        executor_used.execute(() -> {
            total += subSet.size();
            var em      = fact.createEntityManager();
            var session = em.unwrap(Session.class);
            Work work = con -> {
                em.getTransaction().begin();
                try (val stmt = con.prepareStatement(block)) {
                    var changes = stmt.executeUpdate();
                    LOG.info("Changes [" + changes + "][" + total + "]");
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    em.getTransaction().commit();
                    em.close();
                }
            };
            session.doWork(work);
        });
    }
}
