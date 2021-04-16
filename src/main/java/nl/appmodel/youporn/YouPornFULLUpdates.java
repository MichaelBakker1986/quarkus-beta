package nl.appmodel.youporn;

import com.google.common.base.Joiner;
import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.pornhub.Text;
import nl.appmodel.realtime.HibernateUtill;
import nl.appmodel.realtime.LongCall;
import nl.appmodel.realtime.SCVContext;
import nl.appmodel.realtime.Update;
import nl.appmodel.realtime.model.NetworkHash;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.zip.CRC32;
@Log
@ApplicationScoped
public class YouPornFULLUpdates implements Update {
    @Inject              SessionFactory  fact;
    private static final String          zip_url = "https://www.youporn.com/YouPorn-Embed-Videos-Dump.zip";
    private final        Map<Long, Long> hashes  = new ConcurrentHashMap<>();
    private final        Set<Long>       seenIds = new HashSet<>();
    @Inject
    Session         session;
    @Inject
    ManagedExecutor executor;
    Executor executor_used;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new YouPornFULLUpdates();
        updates.executor_used = Executors.newCachedThreadPool();
        updates.fact          = HibernateUtill.em();
        //updates.url = new File("C:\\Users\\michael\\Documents\\Downloads\\YouPorn-Embed-Videos-Dump.zip").toURI().toURL();
        updates.session = nl.appmodel.realtime.HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = updates.lineConsumer(ctx);
        var reader       = updates.readSourceFile('|', lineConsumer);
        updates.preflight(updates.session, ctx, () -> updates.readZip(ctx, reader));
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    private void resolveHashes(Consumer<NetworkHash> consumer) {
        executor_used.execute(() -> {
            session.setFlushMode(FlushModeType.AUTO);
            var query = session.createQuery("SELECT new nl.appmodel.realtime.model.NetworkHash(youporn,crc) from YouPorn order by youporn",
                                            NetworkHash.class);
            query.setLockMode(LockModeType.NONE);
            query.setReadOnly(true);
            try (val stream = query
                    .stream()) {
                stream.forEach(consumer);
            }
        });
    }
    @Scheduled(cron = "0 56 23 * * ?", identity = "new-youporn-videos")
    @Transactional
    @SneakyThrows
    public final void preflight() {
        resolveHashes(hash -> hashes.put(hash.getHash(), hash.getCrc()));
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = this.lineConsumer(ctx);
        var reader       = readSourceFile('|', lineConsumer);
        var zipCall      = readZip(ctx, reader);
        this.preflight(session, ctx, this.prepare(ctx, zipCall));
    }
    public Runnable prepare(SCVContext ctx, LongCall zipCall) {
        var persist = batchPersist(ctx);
        return () -> {
            try {
                long totalLength = zipCall.call();
                persist.run();
                notifier.displayTray("Success - " + getClass().getSimpleName() + " - delete",
                                     "deleted [" + ctx.getChanges() + "] offset [" + 0 + "] total [" + totalLength + "]",
                                     MessageType.INFO);
            } catch (Exception e) {
                e.printStackTrace();
                log.severe("ERROR" + e.getMessage());
                notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
            }
        };
    }
    @SneakyThrows
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            ctx.addExamined();
            try {
                var crc32 = new CRC32();
                crc32.update(String.join("", strings).getBytes(StandardCharsets.UTF_8));
                val youporn = Long.parseLong(strings[9]);
                val $crc    = crc32.getValue();

                if (hashes.containsKey(youporn)) {
                    if ($crc == hashes.get(youporn)) {
                        ctx.addSkipped();
                        return;
                    }
                    hashes.remove(youporn);
                }
                try {

                    var iframe    = strings[0].substring(0, strings[0].indexOf("/iframe") + 8);
                    val preview_d = escape(strings[1]);
                    val header    = escapeStrict(strings[2]);
                    val tag       = escapeStrict(strings[3]);
                    val cat       = escapeStrict(strings[4]);
                    int duration  = INT(sqlNumber(strings[7]));
                    val url       = escape(strings[8]);

                    var $dim  = dims(iframe);
                    var actor = "";
                    if (!strings[5].isEmpty()) {
                        actor = escapeStrict(strings[5]);
                    }

                    var crc32_meta = new CRC32();
                    crc32_meta.update(
                            (tag + cat + actor + "" + header + "" + preview_d + "" + $dim.getW() + "" + $dim.getH() + "" + duration)
                                    .getBytes(StandardCharsets.UTF_8));
                    var $crc_meta = crc32_meta.getValue();

                    ctx.add(
                            "(" + duration + ",\"" + cat + "\",\"" + tag + "\",\"" + header + "\",\"" + preview_d + "\"," + $dim
                                    .getW() + "," + $dim
                                    .getH() + "," + youporn + ",\"" + actor + "\",\"" + url + "\",0," + $crc_meta + "," + $crc + ")");
                    addToBatch(ctx);
                } catch (Exception e) {
                    log.warning("Failed to parse [{}] because [{}]" + Joiner.on("\n").join(strings) + e.getMessage());
                }
            } catch (Exception e) {
                log.severe("Error resolve" + e.getMessage());
            }
        };
    }
    private void addToBatch(SCVContext ctx) {
        if (ctx.getExamined() % 10000 == 0) {
            log.info("Progress Examined:[" + ctx.getExamined() + "] Changes:[" + ctx.getChanges() + "] total:[" + ctx
                    .getTotal() + "] skipped" + ctx.getSkipped());
        }
        if (ctx.size() >= 10000) batchPersist(ctx).run();
    }
    @SneakyThrows
    private Runnable batchPersist(SCVContext ctx) {
        return () -> {
            var rest = ctx.getSqlStatements();
            while (!rest.isEmpty()) {
                val batch_size = Math.min(100000, rest.size());
                val subSet     = new ArrayList<>(rest.subList(0, batch_size));
                rest = rest.subList(batch_size, rest.size());
                persistBatch(ctx, subSet);
            }
            ctx.clear();
        };
    }
    @Transactional
    private void persistBatch(SCVContext ctx, List<String> subSet) {
        val block = """
                    INSERT INTO prosite.youporn (duration,cat,tag,header,preview_d,w,h,youporn,actor,url,flag,$crc_meta,$crc)
                    SELECT * FROM youporn AS new
                    ON DUPLICATE KEY UPDATE
                    duration    =new.duration,
                    url         =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.url, new.url),
                    cat         =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.cat, new.cat),
                    actor       =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.actor, new.actor),  
                    tag         =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.tag, new.tag), 
                    header      =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.header, new.header), 
                    preview_d   =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.preview_d, new.preview_d),
                    $crc_meta   =IF(new.$crc_meta = prosite.youporn.crc_meta, prosite.youporn.$crc_meta, new.$crc_meta), 
                    $crc        =IF(new.$crc      = prosite.youporn.crc,prosite.youporn.$crc,new.$crc),
                    w           =new.w, 
                    h           =new.h,                      
                    updated     =DEFAULT, 
                    prosite.youporn.flag=prosite.youporn.flag & ~6;
                      """.replace("SELECT * FROM youporn AS new", "VALUES " + String.join(",\n", subSet) + " AS new ");
        //what i want to say to the status, meta done, stats done, (if downloaded or not stay that way) , if error/deleted or not stay that way.
        executor_used.execute(() -> {
            ctx.addTotal(subSet.size());
            val em      = fact.createEntityManager();
            val session = em.unwrap(Session.class);
            Work work = con -> {
                em.getTransaction().begin();
                try (val stmt = con.prepareStatement(block)) {
                    var matched = stmt.executeUpdate();
                    log.info(
                            "Matched [" + matched + "][" + ctx.addChanges(stmt.getUpdateCount()) + "][" + ctx.getTotal() + "] skipped" + ctx
                                    .getSkipped() + "/" + ctx.getExamined());
                    em.getTransaction().commit();
                } catch (Exception e) {
                    new Text(block, e.getMessage()).print();
                    em.getTransaction().rollback();
                } finally {
                    em.close();
                }
            };
            session.doWork(work);
        });
    }
}
