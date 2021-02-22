package nl.appmodel;

import com.google.common.base.Joiner;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.Session;
import org.w3c.dom.Document;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
@Slf4j
@ApplicationScoped
public class PornHubUpdates {
    private static final Logger LOG = Logger.getLogger(String.valueOf(PornHubUpdates.class));
    @Inject
    EntityManager em;
    @Inject
    Session       session;
    @SneakyThrows
    private void readPornhubSourceFile() {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder        db  = dbf.newDocumentBuilder();

        log.info("Hello");
        List<String> smts = new ArrayList<>();
        var csvReaderBuilder = new CSVReaderBuilder(new FileReader("G:\\download\\pornhub.com-db\\pornhub.com-db.csv"))
                .withKeepCarriageReturn(true)
                .withCSVParser(
                        new CSVParserBuilder()
                                .withIgnoreQuotations(true)
                                .withSeparator('|')
                                .withStrictQuotes(false)
                                .build()).build();

        try (CSVReader reader = csvReaderBuilder) {
            reader
                    .iterator()
                    .forEachRemaining(strings -> {
                        //System.out.println(Arrays.toString(strings));
                        String   thumb  = strings[1];
                        String   ID     = thumb.split("/")[6];
                        String   iframe = strings[0];
                        Document doc    = null;
                        try {
                            doc = db.parse(new StringBufferInputStream(iframe));
                            doc.getDocumentElement().normalize();
                            String w     = doc.getDocumentElement().getAttribute("height");
                            String h     = doc.getDocumentElement().getAttribute("width");
                            String src   = doc.getDocumentElement().getAttribute("src");
                            String keyId = src.split("/")[4];
                            if (!isNumeric(ID)) {
                                log.info("Not updating: [{}]", String.join(" ", strings));
                            } else {
                                smts.add("UPDATE pornhub set w=" + w + ",h=" + h + ",pornhub_id=" + ID + " where keyid='" + keyId + "';");
                            }
                        } catch (Exception e) {
                            log.error("ERROR", e);
                        }
                        if (csvReaderBuilder.getLinesRead() % 10000 == 0) {
                            log.info("Size: [{}]", csvReaderBuilder.getLinesRead());
                        }
                    });
        } catch (Exception ignored) {
            log.error("ERROR", ignored);
        }
        batchPersist(smts);
    }
    public static boolean isNumeric(String str) {
        // null or empty
        if (str == null || str.length() == 0) {
            return false;
        }
        return str.chars().allMatch(Character::isDigit);
    }
    @SneakyThrows
    public void batchPersist(List<String> smts) {
        var        join     = String.join("\n", smts);
        FileWriter myWriter = new FileWriter("C:\\Users\\michael\\Documents\\pros\\sql\\insert_all.sql");
        myWriter.write(join);
        myWriter.close();
        System.out.println("Successfully wrote to the file.");
        smts.clear();
    }
    @SneakyThrows
    public void goDelete() {
        LOG.info("HELLO");
        LOG.info("job start");
        val         url     = new URL("https://www.pornhub.com/files/deleted.csv");
        var         url1    = new File("H:\\download\\deleted.csv").toURI().toURL();
        var         s       = LegacyUtil.session();
        Set<String> deleted = new HashSet<>(s.createNativeQuery("select pornhub_id from pornhub where deleted=1;").list());
        LOG.info("Got deleted ids");
        Set<String> available = new HashSet<>(s.createNativeQuery("select pornhub_id from pornhub where deleted=0;").list());
        LOG.info("Got cache from database deleted: [" + deleted.size() + "] avail: [" + available.size() + "]");
        var newlyDeleted = new HashSet<String>();

        val in = new BufferedReader(new InputStreamReader(url.openStream()));

        class lineaction {
            int last = 0;
            void parse(String line) {
                var next       = line.split(",");
                var pornhub_id = next[0].trim();
                if ((last++ % 1000) == 0)
                    LOG.info("@" + last);
                if (!deleted.contains(pornhub_id) && available.contains(pornhub_id)) {
                    deleted.add(pornhub_id);
                    newlyDeleted.add(pornhub_id);
                    LOG.info("new deleted record " + pornhub_id);
                }
            }
        }
        var    lineaction = new lineaction();
        String inputLine;
        while ((inputLine = in.readLine()) != null)
            lineaction.parse(inputLine);
        in.close();

        LOG.info(newlyDeleted.size() + " records to be deleted");
        extracted(newlyDeleted);
    }
    public static void main(String[] args) {
        new PornHubUpdates().goDelete();
    }
    @SneakyThrows
    public static void maina(String[] args) {
        log.info("deletedRecord");
        Collection<Object> ids = new ArrayList<>();
        new CSVReader(new FileReader("G:\\download\\deleted.csv")).forEach(csvData -> {
            ids.add(csvData[0]);
        });
        extracted(ids);
    }
    private static void extracted(Collection<?> ids) throws IOException {
        var statements = 0;
        try (val myWriter = new FileWriter("C:\\Users\\michael\\Documents\\pros\\sql\\update_deleted_pornhub_ids.sql")) {
            while (ids.size() > 0) {
                statements++;
                val subS = ids.stream().limit(50000).collect(Collectors.toSet());
                ids.removeAll(subS);
                myWriter.write("UPDATE pornhub set deleted = 1 where pornhub_id in (" + Joiner.on(",").join(subS) + ")\n");
            }
        }
        log.info("Successfully wrote to the file. {} ids in {} statements", ids.size(), statements);
    }
}
