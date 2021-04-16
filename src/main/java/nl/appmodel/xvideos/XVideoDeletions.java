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
public class XVideoDeletions implements Update {
    private static final String zip_url = "https://webmaster-tools.xvideos.com/xvideos.com-deleted-week.csv.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new XVideoDeletions();
        updates.session = HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = updates.lineConsumer(ctx);
        var reader       = updates.readSourceFile('|', lineConsumer);
        var zipCall      = updates.readZip(ctx, reader);
        updates.preflight(updates.session, ctx, updates.prepare(ctx, zipCall));
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    @Scheduled(cron = "0 30 20 * * ?", identity = "deletions-xvideos-videos")
    @Transactional
    @SneakyThrows
    public void preflight() {
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = this.lineConsumer(ctx);
        var reader       = readSourceFile('|', lineConsumer);
        var zipCall      = readZip(ctx, reader);
        preflight(session, ctx, this.prepare(ctx, zipCall));
    }
    public Runnable prepare(SCVContext ctx, LongCall zipCall) {
        val persist = this.batchPersist(ctx);
        return () -> {
            try {
                ctx.reset();
                long totalLength = zipCall.call();
                persist.run();
                notifier.displayTray("Success - " + getClass().getSimpleName() + " - delete",
                                     "deleted [" + ctx.getChanges() + "] offset [" + 0 + "] total [" + totalLength + "]",
                                     MessageType.INFO);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("ERROR", e);
                notifier.displayTray("Fail - " + getClass().getSimpleName() + " - delete", e.getMessage(), MessageType.ERROR);
            } finally {
                ctx.reset();
            }
        };
    }
    @SneakyThrows
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            val url      = escape(strings[1]);
            var embed_id = url.split("/")[3].replaceAll("[^0-9]", "");
            ctx.add(embed_id);
        };
    }
    @SneakyThrows
    private Runnable batchPersist(SCVContext ctx) {
        return () -> {
            if (ctx.noStatements()) return;
            var pro_sql_update = """
                                 UPDATE pro p, xvideos x
                                 SET x.flag = x.flag | 256,
                                     x.updated = DEFAULT
                                 WHERE x.xvideos IN (%s)  
                                 """.formatted(String.join(",", ctx.getSqlStatements()));
            ctx.addChanges(session.createNativeQuery(pro_sql_update)
                                  .executeUpdate());
        };
    }
}
