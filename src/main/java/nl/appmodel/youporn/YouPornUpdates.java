package nl.appmodel.youporn;

import com.google.common.base.Joiner;
import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
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
import java.net.URL;
import java.time.LocalDate;
import java.util.function.Consumer;
import java.util.zip.ZipInputStream;
@Slf4j
@ApplicationScoped
public class YouPornUpdates implements Update {
    private static final String zip_url = "https://www.youporn.com/YouPorn-Embed-Videos-Dump.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new YouPornUpdates();
        //updates.url = new File("C:\\Users\\michael\\Documents\\Downloads\\YouPorn-Embed-Videos-Dump.zip").toURI().toURL();
        updates.session = nl.appmodel.realtime.HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = updates.lineConsumer(ctx);
        var reader       = updates.readSourceFile('|', lineConsumer);
        var zipCall      = updates.readZip(ctx, reader);
        updates.prepare(ctx, zipCall);
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    @Scheduled(cron = "0 56 23 * * ?", identity = "new-youporn-videos")
    @Transactional
    @SneakyThrows
    public final void preflight() {
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = this.lineConsumer(ctx);
        var reader       = readSourceFile('|', lineConsumer);
        var zipCall      = readZip(ctx, reader);
        this.preflight(session, ctx, this.prepare(ctx, zipCall));
    }
    public Runnable prepare(SCVContext ctx, LongCall zipCall) {
        val persist = this.batchPersist(ctx);
        return () -> {
            try {
                ctx.reset();
                long nBytesOffset = Long.parseLong(String.valueOf(
                        session.createNativeQuery(
                                "SELECT IFNULL((SELECT value FROM prosite.marker c WHERE c.name=:name),:default_offset)")
                               .setParameter("name", getClass().getSimpleName().toLowerCase() + "_file_cursor")
                               .setParameter("default_offset", 0)
                               .getSingleResult()));
                ctx.withLength(nBytesOffset);
                long totalLength = zipCall.call();
                persist.run();
                if (totalLength > nBytesOffset)
                    session.createNativeQuery("REPLACE INTO prosite.marker VALUES (:name,:totalLength)")
                           .setParameter("name", getClass().getSimpleName().toLowerCase() + "_file_cursor")
                           .setParameter("totalLength", String.valueOf(totalLength))
                           .executeUpdate();
                notifier.displayTray("Success - " + getClass().getSimpleName(),
                                     "changes [" + ctx.getChanges() + "] offset [" + nBytesOffset + "] total [" + totalLength + "]",
                                     MessageType.INFO);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("ERROR", e);
                notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
            } finally {
                ctx.reset();
            }
        };
    }
    @SneakyThrows
    public LongCall readZip(SCVContext ctx, Consumer<Reader> reader) {
        return () -> {
            var totalLength = 0L;
            var offset      = ctx.getContent_length_offset();
            try (var bis = new BufferedInputStream(ctx.getUrl().openStream());
                 var zis = new ZipInputStream(bis)) {
                //we will only use the first entry
                var ze = zis.getNextEntry();
                //sure this will be only one file..
                log.info("File: {} Size: {} Last Modified {}", ze.getName(), ze.getSize(),
                         LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY));
                var skipped = 0L;
                if (ze.getSize() > offset) {
                    totalLength = ze.getSize();
                    long safe_offset = Math.min(ze.getSize(), Math.max(0L, offset - 1000L)); //
                    //how can JAVA write such bad API, skip (long) is chopped by INTEGER.MAX_VALUE. (why not just take an INTEGER instead..)
                    if (safe_offset > 0)
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
    public Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            try {
                var iframe    = strings[0].substring(0, strings[0].indexOf("/iframe") + 8);
                val code      = escape(iframe);
                val preview_d = escape(strings[1]);
                val header    = escape(strings[2]);
                val tags      = escape(strings[3]);
                val cat       = escape(strings[4]);
                int duration  = Integer.parseInt(strings[7]);
                val url       = escape(strings[8]);
                val youporn   = Long.parseLong(strings[9]);

                var dims  = dims(iframe);
                var actor = "";
                if (!strings[5].isEmpty()) {
                    actor = escape(strings[5]);
                }
                ctx.getSqlStatements().add(
                        "(\"" + code + "\"," + duration + ",\"" + cat + "\",\"" + tags + "\",\"" + header + "\",\"" + preview_d + "\"," + dims
                                .getW() + "," + dims.getH() + "," + youporn + ",\"" + actor + "\",\"" + url + "\")");
            } catch (Exception e) {
                log.warn("Failed to parse [{}] because [{}]", Joiner.on("\n").join(strings), e.getMessage());
            }
        };
    }
    @SneakyThrows
    private Runnable batchPersist(SCVContext ctx) {
        return () -> {
            if (ctx.getSqlStatements().isEmpty()) return;
            var sql = """
                      INSERT INTO prosite.youporn (code,duration_ui,duration,cat,tag,header,preview_d,w,h,youporn,actor,url)
                      SELECT * FROM youporn AS new 
                      ON DUPLICATE KEY UPDATE 
                      code=new.code, 
                      duration=new.duration, 
                      cat=new.cat, 
                      tag=new.tag, 
                      header=new.header, 
                      preview_d=new.preview_d, w=new.w, h=new.h, youporn=new.youporn, actor=new.actor, url=new.url, updated=DEFAULT, 
                      prosite.youporn.flag=prosite.youporn.flag & ~6;
                      """.replace("SELECT * FROM youporn AS new", "VALUES " + String.join(",\n", ctx.getSqlStatements()) + " AS new ");
            ctx.addChanges(session.createNativeQuery(sql)
                                  .executeUpdate());
        };
    }
}
