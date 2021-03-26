package nl.appmodel.pornhub;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.PornHubHash;
import nl.appmodel.realtime.HibernateUtil;
import nl.appmodel.realtime.Update;
import org.apache.commons.codec.binary.Hex;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;
@Log
@ApplicationScoped
public class FullPornHubUpdates implements Update {
    private       int          changes       = 0;
    private       long         update_time   = new Date().getTime();
    private final List<String> sqlStatements = new ArrayList<>();
    private final Set<Long>    seenIds       = new HashSet<>();
    private       long         skipped       = 0, examinedRecords = 0;
    private              URL               url;
    private static final char              separator = '|';
    private final        Map<Long, String> hashes    = new ConcurrentHashMap<>();
    static               MessageDigest     MD5       = Update.sneak(() -> MessageDigest.getInstance("MD5"));
    String URL   = "https://www.pornhub.com/files/pornhub.com-db.zip";
    int    total = 0;
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
        pornHubUpdates.URL           = "C:\\Users\\michael\\Documents\\Downloads\\pornhub.com-db.zip";
        pornHubUpdates.url           = new File(pornHubUpdates.URL).toURI().toURL();
        pornHubUpdates.executor_used = Executors.newCachedThreadPool();
        pornHubUpdates.fact          = HibernateUtil.em();
        pornHubUpdates.session       = HibernateUtil.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.preflight_skip_dl();
    }
    public void cleanUp() {
        examinedRecords = 0;
        skipped         = 0;
        changes         = 0;
        seenIds.clear();
        hashes.clear();
        session.close();
        sqlStatements.clear();
    }
    private void resolveHashes(Consumer<PornHubHash> consumer) {
        executor_used.execute(() -> {
            try (val stream = session.createQuery(
                    "SELECT new nl.appmodel.PornHubHash(pornhub_id,changes_hash) from PornHub order by pornhub_id", PornHubHash.class)
                                     .stream()) {
                stream.forEach(consumer);
            }
        });
    }
    @SneakyThrows
    public void preflight_skip_dl() {
        update_time = new Date().getTime();
        sqlStatements.clear();
        MD5 = MessageDigest.getInstance("MD5");
        if (executor_used == null) executor_used = executor;
        if (url == null) url = new URL(URL);
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub_id(), pornHubHash.getChanges_hash()));
        this.pornhubVideos(-1L);
        batchPersist();
        setUnavailableStatus();
        cleanUp();
    }
    //    @Scheduled(cron = "0 09 02 * * ?", identity = "full-pornhub-videos-update")
    @Transactional
    @SneakyThrows
    public void preflight() {
        update_time = new Date().getTime();
        sqlStatements.clear();
        MD5 = MessageDigest.getInstance("MD5");
        if (executor_used == null) executor_used = executor;
        if (url == null) url = new URL(URL);
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub_id(), pornHubHash.getChanges_hash()));
        preflight(session, this::pornhubVideos, url);
        batchPersist();
        setUnavailableStatus();
        cleanUp();
    }
    private void setUnavailableStatus() {
        val keys = hashes.keySet();
        keys.removeAll(seenIds);
        val collect = keys.stream().map(aLong -> String.valueOf(aLong)).collect(Collectors.joining(","));
        var nativeQuery = session.createNativeQuery(
                "UPDATE pornhub set status = 10,updated=%s where status < 9 AND pornhub_id in (%s)".formatted(collect, update_time));
        var changes = nativeQuery.executeUpdate();
        log.info("Unavailable records:" + changes);
    }
    @SneakyThrows
    public void pornhubVideos(Long offset) {
        try (val bis = new BufferedInputStream(url.openStream());
             val zis = new ZipInputStream(bis)) {
            val ze = zis.getNextEntry();
            log.info("File: %s Size: %s Last Modified %s"
                             .formatted(ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY)));
            try (val br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
                readSourceFile(separator, br, this::readPornhubSourceFileEntry);
            }
        }
    }
    public long pornhubIdFromThumb(String in) {
        val split = in.substring(10, 64).split("/");
        if (split.length < 4) return -in.length();
        return sqlNumber(split[4]);
    }
    @ToString
    @AllArgsConstructor
    class PornhubRow {
        long   pornhub_id;
        String code, header, tag, cat;
        long duration, views, up, down;
        String picture_d, preview_d;
        private String keyId(Dim dim) {
            return escape(dim.getSrc().split("/")[4]);
        }
        public String sql() {
            val dim = dims(this.code);
            return "(" + up + "," + down + "," + views + "," + duration + ",'" + cat + "',\"" + tag + "\",\"" + header + "\",\"" + picture_d + "\",\"" + preview_d + "\"," + dim
                    .getW() + "," + dim.getH() + "," + pornhub_id + ",'" + keyId(dim) + "'," + update_time + ",1,\"" + hash() + "\")";
        }
        public String hash() {
            return new String(
                    Hex.encodeHex(MD5.digest((tag + cat + views + "" + up + "" + down + "" + 0).getBytes(StandardCharsets.UTF_8))));
        }
    }
    @SneakyThrows
    private void readPornhubSourceFileEntry(String[] strings) {
        examinedRecords++;
        val pornhub_id = pornhubIdFromThumb(strings[1]);
        if (strings.length < 13 ||
            Arrays.stream(strings).anyMatch(s -> s == null) ||
            pornhubIdFromThumb(strings[11]) != pornhub_id ||
            pornhub_id < 0
        ) {
            log.warning("Invalid record, skipping [" + String.join(" ", strings) + "]");
            return;
        }
        try {
            addToBatch(pornhub_id, new PornhubRow(pornhub_id,
                                                  strings[0],
                                                  escapeStrict(strings[3]),
                                                  escapeStrict(strings[4]),
                                                  escapeStrict(strings[5]),
                                                  sqlNumber(strings[7]),
                                                  sqlNumber(strings[8]),
                                                  sqlNumber(strings[9]),
                                                  sqlNumber(strings[10]),
                                                  escape(strings[11]),
                                                  escape(strings[12]))
                      );
        } catch (Exception e) {
            log.warning("Invalid record " + String.join(" ", strings));
        }
    }
    private void addToBatch(long pornhub_id, PornhubRow row) {
        if (hashes.containsKey(pornhub_id)) {
            if (row.hash().equals(hashes.get(pornhub_id))) {
                skipped++;
            } else {
                sqlStatements.add(row.sql());
            }
            hashes.remove(pornhub_id);
        } else {
            sqlStatements.add(row.sql());
            seenIds.add(pornhub_id);
        }
        if (sqlStatements.size() >= 10000) batchPersist();
        log.info("Progress Examined:[" + examinedRecords + "] Changes:[" + changes + "] total:[" + total + "] skipped" + skipped);
    }
    /**
     * batch large request synchronous partitioning
     */
    @SneakyThrows
    private void batchPersist() {
        var rest = sqlStatements;
        while (!rest.isEmpty()) {
            val batch_size = Math.min(100000, rest.size());
            val subSet     = new ArrayList<>(rest.subList(0, batch_size));
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
        val block = """
                    INSERT INTO prosite.pornhub (up,down,views,duration,cat,tag,header,picture_d,preview_d,w,h,pornhub_id,keyid,updated,status,changes_hash) VALUES
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
                    updated=new.updated, 
                    changes_hash=new.changes_hash,
                    prosite.pornhub.status=IF(prosite.pornhub.picture_d != new.picture_d,5,  
                                                   IF(prosite.pornhub.status=2 OR prosite.pornhub.status=4,4,prosite.pornhub.status)
                                                   ));
                    """.formatted(String.join(",\n", subSet));

        executor_used.execute(() -> {
            total += subSet.size();
            val em      = fact.createEntityManager();
            val session = em.unwrap(Session.class);
            Work work = con -> {
                em.getTransaction().begin();
                try (val stmt = con.prepareStatement(block)) {
                    var matched = stmt.executeUpdate();
                    log.info("Matched [" + matched + "][" + (changes += stmt
                            .getUpdateCount()) + "][" + total + "] skipped" + skipped + "/" + examinedRecords);
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
    @AllArgsConstructor
    class Text {
        String story, err;
        public void print() {
            try {
                Matcher m = Pattern.compile("at line ([0-9]+)").matcher(this.err);   // the pattern to search for
                m.find();
                var lines = Integer.parseInt(m.group(1));
                val split = this.story.split("\n");
                log.info(split[lines - 1] + "\n" + split[lines - 1] + '\n' + split[lines + 1]);
            } catch (Exception e) {
                log.warning("Error" + e);
            }
        }
    }
    record Text2(String story, String err) {
        public void print() {
            try {
                Matcher m = Pattern.compile("at line ([0-9]+)").matcher(this.err);   // the pattern to search for
                m.find();
                var lines = Integer.parseInt(m.group(1));
                val split = this.story.split("\n");
                log.info(split[lines - 1] + "\n" + split[lines - 1] + '\n' + split[lines + 1]);
            } catch (Exception e) {
                log.warning("Error" + e);
            }
        }
    }
}
