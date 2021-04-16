package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.realtime.HibernateUtill;
import nl.appmodel.realtime.LongCall;
import nl.appmodel.realtime.SCVContext;
import nl.appmodel.realtime.Update;
import org.hibernate.Session;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
import java.io.BufferedInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.zip.ZipInputStream;
@Log
@ApplicationScoped
public class PornHubUpdates implements Update {
    private String url = "https://www.pornhub.com/files/pornhub.com-db.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new PornHubUpdates();
        updates.session = HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        //   var url = new File("C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip").toURI().toURL();
        var ctx            = new SCVContext(new URL("https://www.pornhub.com/files/pornhub.com-db.zip"));
        var readerConsumer = updates.readSourceFile('|', updates.lineConsumer(ctx));
        var zipCall        = updates.readZip(ctx, readerConsumer);
        updates.prepare(ctx, () -> zipCall.call());
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    @Scheduled(cron = "0 56 23 * * ?", identity = "new-pornhub-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        var url = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value FROM prosite.cursors c WHERE c.name='pornhub_videos_file_last_modified'),0)")
                       .getSingleResult()));

        var connection      = (HttpURLConnection) url.openConnection();
        var headerFieldSize = connection.getHeaderField("content-length");
        var lastModified    = connection.getHeaderField("last-modified");
        var content_length  = Long.parseLong(connection.getHeaderField("content-length"));
        long headerModifiedUTC = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                                              .toEpochMilli();
        connection.getInputStream().close();
        if (headerModifiedUTC != cachedLastModified) {
            var ctx            = new SCVContext(url);
            var readerConsumer = readSourceFile('|', this.lineConsumer(ctx));
            var zipCall        = this.readZip(ctx, readerConsumer);
            prepare(ctx.withTotalLength(content_length), () -> zipCall.call());
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES ('pornhub_videos_file_last_modified',:pornhub_videos_file_last_modified)")
                   .setParameter(
                           "pornhub_videos_file_last_modified", String.valueOf(headerModifiedUTC)).executeUpdate();
        } else {
            notifier.displayTray("Success - Pornhub - updates", "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
    public void prepare(SCVContext ctx, Runnable runnable) {
        try {
            var cursor_name = getClass().getSimpleName().toLowerCase() + "_cursor";
            ctx.withLength(Long.parseLong(String.valueOf(
                    session.createNativeQuery(
                            "SELECT IFNULL((SELECT value FROM prosite.cursors c WHERE c.name=:cursor_name),10930072417)")
                           .setParameter("cursor_name", cursor_name)
                           .getSingleResult())));
            runnable.run();
            batchPersist(ctx);
            if (ctx.getContent_length() > ctx.getContent_length_offset())
                session.createNativeQuery("REPLACE INTO prosite.cursors VALUES (:cursor_name,:totalLength)")
                       .setParameter("totalLength", String.valueOf(ctx.getContent_length()))
                       .setParameter("cursor_name", cursor_name)
                       .executeUpdate();
            notifier.displayTray("Success - " + getClass().getSimpleName(),
                                 "changes [" + ctx.getChanges() + "] offset [" + ctx
                                         .getContent_length_offset() + "] total [" + ctx.getContent_length() + "]",
                                 MessageType.INFO);
        } catch (Exception e) {
            log.severe("ERROR" + e.getMessage());
            notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
        }
    }
    @SneakyThrows
    public LongCall readZip(SCVContext ctx, Consumer<Reader> reader) {
        return () -> {
            long totalLength = 0;
            try (var bis = new BufferedInputStream(ctx.getUrl().openStream());
                 var zis = new ZipInputStream(bis)) {
                var ze = zis.getNextEntry();
                log.info("File: {} Size: {} Last Modified {}" + new Object[]{ze.getName(), ze.getSize(),
                                                                             LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY)});
                var skipped = 0L;
                if (ze.getSize() > ctx.getContent_length_offset()) {
                    totalLength = ze.getSize();

                    long safe_offset = Math.max(0, ctx.getContent_length_offset() - 1000); //
                    //how can JAVA write such bad API, skip (long) is chopped by INTEGER.MAX_VALUE. (why not just take an INTEGER instead..)
                    while ((skipped += zis.skip(safe_offset - skipped)) < safe_offset) ;
                    var remainder  = new String(zis.readAllBytes());
                    var first_lb   = remainder.indexOf('\n');
                    var usefulPart = remainder.substring(first_lb + 1);
                    try (var read = new StringReader(usefulPart)) {
                        reader.accept(read);
                    }
                } else {
                    log.info("No new data found");
                }
            }
            return totalLength;
        };
    }
    @SneakyThrows
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            val preview_d = CONCAT(escape(strings[11]), escape(strings[12]));
            val pornhub   = strings[1].split("/")[6];
            val header    = escape(strings[3]);
            val tags      = escape(strings[4]);
            val cat       = escape(strings[5]);
            val duration  = sqlNumber(strings[7]);
            val views     = sqlNumber(strings[8]);
            val up        = sqlNumber(strings[9]);
            val down      = sqlNumber(strings[10]);
            var dims      = dims(strings[0]);
            val keyId     = escape(dims.getSrc().split("/")[4]);
            if (!isNumeric(pornhub)) {
                log.info("Not updating: [{}]" + String.join(" ", strings));
            } else {
                ctx.add(
                        "(" + up + "," + down + "," + views + "," + duration + ",'" + cat + "',\"" + tags + "\",\"" + header + "\",\"" + preview_d + "\"," + dims
                                .getW() + "," + dims.getH() + "," + pornhub + ",'" + keyId + "',0)");
            }
        };
    }
    @SneakyThrows
    private void batchPersist(SCVContext ctx) {
        if (ctx.isEmpty()) return;
        var sql = """
                  INSERT INTO prosite.pornhub (up,down,views,duration,cat,tag,header,preview_d,w,h,pornhub,keyid,flag)
                  SELECT * FROM pornhub AS new
                  ON DUPLICATE KEY UPDATE 
                  up=new.up,
                  down=new.down,
                  views=new.views, 
                  duration=new.duration, 
                  cat=new.cat, 
                  tag=new.tag, 
                  header=new.header, 
                  preview_d=new.preview_d, 
                  w=new.w, 
                  h=new.h, 
                  updated=DEFAULT 
                        """.replace("SELECT * FROM pornhub AS new", "VALUES " + String.join(",\n", ctx.getSqlStatements()) + " AS new ");
        ctx.addChanges(session.createNativeQuery(sql)
                              .executeUpdate());
    }
}
