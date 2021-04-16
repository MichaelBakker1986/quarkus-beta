package nl.appmodel.xvideos;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
import java.net.URL;
import java.util.function.Consumer;
@Slf4j
@ApplicationScoped
public class XVideoUpdates implements Update {
    private static final String zip_url = "https://webmaster-tools.xvideos.com/xvideos.com-export-week.csv.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new XVideoUpdates();
        //   var url = new File("C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip").toURI().toURL();
        updates.session = HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = updates.lineConsumer(ctx);
        var reader       = updates.readSourceFile(';', lineConsumer);
        updates.preflight(updates.session, ctx, () -> updates.readZip(ctx, reader));
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    @Scheduled(cron = "0 00 18 * * ?", identity = "new-xvideos-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = this.lineConsumer(ctx);
        var reader       = readSourceFile(';', lineConsumer);
        var zipCall      = readZip(ctx, reader);
        preflight(session, ctx, this.prepare(ctx, zipCall));
    }
    public Runnable prepare(SCVContext ctx, LongCall zipCall) {
        var persist = batchPersist(ctx);
        return () -> {
            try {
                ctx.reset();
                long totalLength = zipCall.call();
                persist.run();
                notifier.displayTray("Success - " + getClass().getSimpleName(),
                                     "changes [" + ctx.getChanges() + "] offset [" + 0 + "] total [" + totalLength + "]",
                                     MessageType.INFO);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("ERROR", e);
                notifier.displayTray("Fail - " + getClass().getSimpleName() + " - updates", e.getMessage(), MessageType.ERROR);
            } finally {
                ctx.reset();
            }
        };
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
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (String[] strings) -> {
            val header    = escape(strings[1]);
            val preview_d = escape(strings[3]);
            val dim       = dims(strings[4]);
            val tags      = escape(strings[5]);
            val actor     = escape(strings[6]);
            val xvideos   = escape(strings[7]);
            val cat       = escape(strings[8]);
            val duration  = sqlNumber(escape(strings[2]));

            ctx.add(
                    "(\"," + duration + ",\"" + cat + "\",\"" + tags + "\",\"" + header + "\",\"" + preview_d + "\"," + dim
                            .getW() + "," + dim.getH() + ",\"" + actor + "\"," + xvideos + ")");
        };
    }
    @SneakyThrows
    private Runnable batchPersist(SCVContext ctx) {
        return () -> {
            if (ctx.isEmpty()) return;
            var sql = """
                                INSERT INTO prosite.xvideos (duration,cat,tag,header,preview_d,w,h,actor,xvideos)
                                SELECT * FROM xvideos AS new 
                                ON DUPLICATE KEY UPDATE 
                                duration=new.duration, 
                                cat=new.cat, 
                                tag=new.tag, 
                                header=new.header, 
                                preview_d=new.preview_d, 
                                w=new.w, 
                                h=new.h, 
                                actor=new.actor, 
                                updated=DEFAULT, 
                                prosite.xvideos.flag=flag & ~6;
                      """.replace("SELECT * FROM xvideos AS new", "VALUES " + String.join(",\n", ctx.getSqlStatements()) + " AS new ");
            ctx.addChanges(session.createNativeQuery(sql).executeUpdate());
        };
    }
}
