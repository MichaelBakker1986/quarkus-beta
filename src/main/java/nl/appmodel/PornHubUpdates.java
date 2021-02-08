package nl.appmodel;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.quarkus.runtime.Startup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.w3c.dom.Document;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
@Slf4j
@Startup
@ApplicationScoped
public class PornHubUpdates {
    @Inject
    EntityManager em;
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
    public static void main(String[] args) {
        log.info("deletedRecord");
        Collection<String> ids = new ArrayList<>();
        new CSVReader(new FileReader("G:\\download\\deleted.csv")).forEach(csvData -> {
            String pornhub_id = csvData[0];
            ids.add(pornhub_id);
        });
        val udpate_stms = new ArrayList<String>();
        while (ids.size() > 0) {
            Set<String> subS = ids.stream().limit(50000).collect(Collectors.toSet());
            ids.removeAll(subS);
            String sql = "UPDATE pornhub set deleted = 1 where pornhub_id in (" + String.join(",", subS) + ")";
            udpate_stms.add(sql);
        }
        FileWriter myWriter = new FileWriter("C:\\Users\\michael\\Documents\\pros\\sql\\update_deleted_pornhub_ids.sql");
        myWriter.write(String.join(";\n", udpate_stms));
        myWriter.close();
        System.out.println("Successfully wrote to the file.");
    }
}
