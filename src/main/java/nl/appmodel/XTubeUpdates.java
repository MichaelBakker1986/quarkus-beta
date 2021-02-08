package nl.appmodel;

import com.opencsv.CSVReader;
import io.quarkus.runtime.Startup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import javax.enterprise.context.ApplicationScoped;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
@Slf4j
@Startup
@ApplicationScoped
public class XTubeUpdates {
    @SneakyThrows
    public static void main(String[] args) {
        log.info("deletedRecord");
        Collection<String> ids = new ArrayList<>();
        new CSVReader(new FileReader("G:\\download\\Xtube-Deleted-Videos.csv"))
                .forEach(csvData -> {
                    var split = csvData[0].split("/");
                    if (split.length > 4) {
                        val last       = split[split.length - 1].split("-");
                        val network_id = last[last.length - 1];
                        ids.add(network_id);
                    }
                });

        List<String> udpate_stms = new ArrayList<>();
        while (ids.size() > 0) {
            val subS = ids.stream().limit(100000).collect(Collectors.toSet());
            ids.removeAll(subS);
            String sql = "UPDATE xtube set deleted = 1 where xtube_id in (" + String.join(",", subS) + ")";
            udpate_stms.add(sql);
        }
        val myWriter = new FileWriter("C:\\Users\\michael\\Documents\\pros\\sql\\update_deleted_xtube_ids.sql");
        myWriter.write(String.join(";\n", udpate_stms));
        myWriter.close();
        log.info("Successfully wrote to the file.");
    }
}
