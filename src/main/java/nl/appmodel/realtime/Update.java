package nl.appmodel.realtime;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import lombok.SneakyThrows;
import org.hibernate.Session;
import java.awt.TrayIcon.MessageType;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
public interface Update {
    Notifier notifier     = new Notifier();
    Long     MILLS_IN_DAY = 86400000L;
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
}
