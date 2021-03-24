package nl.appmodel.realtime;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.hibernate.Session;
import java.awt.TrayIcon.MessageType;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
public interface Update {
    Logger   LOG          = Logger.getLogger(String.valueOf(Update.class));
    Notifier notifier     = new Notifier();
    Long     MILLS_IN_DAY = 86400000L;
    @SneakyThrows
    default void readSourceFile(char sep, Reader in_reader, Consumer<String[]> consumer) {
        var csvReaderBuilder = new CSVReaderBuilder(in_reader)
                .withKeepCarriageReturn(true)
                .withCSVParser(
                        new CSVParserBuilder()
                                .withIgnoreQuotations(true)
                                .withSeparator(sep)
                                .withStrictQuotes(false)
                                .build()).build();

        csvReaderBuilder
                .iterator()
                .forEachRemaining(consumer);
    }
    @SneakyThrows
    default void preflight(Session session, Consumer<Long> runnable, URL url) {
        this.preflight(getClass().getSimpleName().toLowerCase(), session, runnable, url);
    }
    @SneakyThrows
    default void preflight(String param_name, Session session, Consumer<Long> runnable, URL url) {
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value from prosite.cursors c where c.name=:param_name),0)")
                       .setParameter("param_name", param_name)
                       .getSingleResult()));

        var connection      = (HttpURLConnection) url.openConnection();
        var headerFieldSize = connection.getHeaderField("content-length");
        var lastModified    = connection.getHeaderField("last-modified");
        var contentLength   = Long.parseLong(connection.getHeaderField("content-length"));

        long headerModifiedUTC = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                                              .toEpochMilli();
        connection.getInputStream().close();
        if (headerModifiedUTC != cachedLastModified) {
            runnable.accept(contentLength);
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES (:param_name,:file_last_modified)")
                   .setParameter("param_name", param_name)
                   .setParameter("file_last_modified", String.valueOf(headerModifiedUTC))
                   .executeUpdate();
        } else {
            notifier.displayTray("Success - " + getClass().getName().toLowerCase(),
                                 "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
    @AllArgsConstructor
    @Getter
    class Dim {
        long w, h;
        String src;
    }
    default Dim dims(String code) {
        var matches = Pattern.compile("(\\S+)=[\"']?((?:.(?![\"']?\\s+(?:\\S+)=|\\s*\\/?[>\"']))+.)[\"']?")
                             .matcher(code)
                             .results()
                             .map(MatchResult::group)
                             .collect(Collectors.toMap(o -> trim(o.split("=")[0]).toLowerCase(), o -> escape(trim(o.split("=")[1]))));

        val w   = sqlNumber(matches.get("width"));
        val h   = sqlNumber(matches.get("height"));
        val src = matches.get("src");
        return new Dim(w, h, src);
    }
    default String trim(String s) {
        return s.replaceAll("^[\"' ]+|[\"' ]+$", "");
    }
    static void main(String[] args) {
        //  var dims = new Update() {}.dims("testabsd width='100' height=100");
        var upd = new Update() {};
        var matches = Pattern.compile("(\\S+)=[\"']?((?:.(?![\"']?\\s+(?:\\S+)=|\\s*\\/?[>\"']))+.)[\"']?")
                             .matcher("testabsd async width='100' height=100 src=\"213more\"")
                             .results()
                             .map(MatchResult::group)
                             .collect(Collectors.toMap(o -> o.split("=")[0],
                                                       o -> upd.escape(o.split("=")[1].replaceAll("^[\"' ]+|[\"' ]+$", ""))));

        System.out.println(matches.entrySet());
    }
    default long sqlNumber(String number) {
        if (number == null) return -1;
        return Long.parseLong(number.replaceAll("[^0-9]", ""));
    }
    default String escape(String in) {
        if (in == null) return "";
        return in
                .replaceAll("[{\\[]", "(")
                .replaceAll("[}\\]]", ")")
                .replaceAll(";", ",")
                .replaceAll("\"", "'");
    }
    default boolean isNumeric(String str) {
        // null or empty
        if (str == null || str.length() == 0) {
            return false;
        }
        return str.chars().allMatch(Character::isDigit);
    }
}
