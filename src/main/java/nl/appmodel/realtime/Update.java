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
    Notifier notifier = new Notifier();
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
    default void readPornhubSourceFile(char sep, Reader in_reader, Consumer<String[]> consumer) {
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
    default void preflight(Session session, Runnable runnable, URL url) {
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value from prosite.cursors c where c.name=:name),0)")
                       .setParameter("name", getClass().getSimpleName().toLowerCase())
                       .getSingleResult()));

        var connection      = (HttpURLConnection) url.openConnection();
        var headerFieldSize = connection.getHeaderField("content-length");
        var lastModified    = connection.getHeaderField("last-modified");
        long headerModifiedUTC = ZonedDateTime.parse(lastModified, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant()
                                              .toEpochMilli();
        connection.getInputStream().close();
        if (headerModifiedUTC != cachedLastModified) {
            runnable.run();
            session.createNativeQuery(
                    "REPLACE INTO prosite.cursors VALUES (:name,:file_last_modified)")
                   .setParameter("name", getClass().getName().toLowerCase())
                   .setParameter("file_last_modified", String.valueOf(headerModifiedUTC))
                   .executeUpdate();
        } else {
            notifier.displayTray("Success - " + getClass().getName().toLowerCase(),
                                 "No changes since [" + lastModified + "] size:[" + headerFieldSize + "]",
                                 MessageType.INFO);
        }
    }
}
