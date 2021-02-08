package nl.appmodel;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class SplitCSV {
    @SneakyThrows
    public static void main(String[] args) {
        File     file   = new File("C:\\Users\\michael\\Documents\\pros\\sql\\insert_all.sql");
        String[] result = Files.asCharSource(file, Charsets.UTF_8).read().split("\n");
        writeFile(8, 0, result);
        writeFile(8, 1, result);
        writeFile(8, 2, result);
        writeFile(8, 3, result);
        writeFile(8, 4, result);
        writeFile(8, 5, result);
        writeFile(8, 6, result);
        writeFile(8, 7, result);
    }
    @SneakyThrows
    private static void writeFile(int offset, int i, String[] result) {
        List<String> lines = new ArrayList<>();
        for (int j = 30000; j < result.length; j += (offset + i)) {
            lines.add(result[j].replace(";", " and pornhub_id=0;"));
        }
        File file = new File("C:\\Users\\michael\\Documents\\pros\\sql\\insert_all" + i + ".sql");
        Files.write(String.join("\n", lines).getBytes(), file);
    }
}
