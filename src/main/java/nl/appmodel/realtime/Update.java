package nl.appmodel.realtime;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.text.StringEscapeUtils;
import org.hibernate.Session;
import java.awt.TrayIcon.MessageType;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;
public interface Update {
    Logger   LOG          = Logger.getLogger(String.valueOf(Update.class));
    Notifier notifier     = new Notifier();
    Long     MILLS_IN_DAY = 86400000L;
    @SneakyThrows
    default Consumer<Reader> readSourceFile(char sep, Consumer<String[]> consumer) {
        return (in_reader) -> {
            var csvReaderBuilder = new CSVReaderBuilder(in_reader)
                    .withKeepCarriageReturn(true)
                    .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                    .withCSVParser(
                            new CSVParserBuilder()
                                    .withEscapeChar((char) 0)
                                    .withIgnoreQuotations(true)
                                    .withSeparator(sep)
                                    .withStrictQuotes(false)
                                    .build()).build();
            var iterator = csvReaderBuilder
                    .iterator();
            while (iterator.hasNext()) {
                consumer.accept(iterator.next());
            }
        };
    }
    @SneakyThrows
    default void preflight(Session session, SCVContext ctx, Runnable zipCall) {
        this.preflight(getClass().getSimpleName().toLowerCase(), session, ctx.getUrl(), ctx, zipCall);
    }
    @SneakyThrows
    default void preflight(String param_name, Session session, URL url, SCVContext ctx, Runnable zipCall) {
        long cachedLastModified = Long.parseLong(String.valueOf(
                session.createNativeQuery(
                        "SELECT IFNULL((SELECT value FROM prosite.marker c WHERE c.name=:param_name),0)")
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
            ctx.setContentLength(contentLength);
            zipCall.run();
            session.createNativeQuery(
                    "REPLACE INTO prosite.marker VALUES (:param_name,:file_last_modified)")
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
        return s.replaceAll("^[\"`' ]+|[\"`' ]+$", "");
    }
    @SneakyThrows
    static void main(String[] args) {
        String s =
                StringEscapeUtils.unescapeHtml4(new String("""
                                                               , '[ÒÓÔÕÖØ]', 'O')
                                                                ,'[ÙÚÛÜ]','U')
                                                           ,'[ÌÍÎÏ¡Ÿ]','I')
                                                           ,'[ÈÉÊË£]','E')
                                                           ,'[ÀÁâÃÄÅÆ©@]','A')
                                                           """
                                                                   .replaceAll("[ÒÓÔÕÖØ]", "O")
                                                                   .replaceAll("[òóôõöø]", "o")
                                                                   .replaceAll("[ÙÚÛÜ]", "U")
                                                                   .replaceAll("[ùúûü]", "u")
                                                                   .replaceAll("[Ÿ¥]", "Y")
                                                                   .replaceAll("[ÿ¥]", "y")
                                                                   .replaceAll("[ÌÍÎÏİI¡]", "I")
                                                                   .replaceAll("[ìíîïi̇i¡]", "i")
                                                                   .replaceAll("[ÈÉÊË£]", "E")
                                                                   .replaceAll("[èéêë£]", "e")
                                                                   .replaceAll("[©]", "C")
                                                                   .replaceAll("[Æ]", "AE")
                                                                   .replaceAll("[ÀÁÂÃÄÅĄ]", "A")
                                                                   .replaceAll("[àáâãäåą]", "a")
                                                                   .getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8));

        Files.write(Paths.get("cyrillic.txt"),
                    ("\uFEFF" + s).getBytes(StandardCharsets.UTF_8));

        //  var dims = new Update() {}.dims("testabsd width='100' height=100");
        var upd = new Update() {};
        var matches = Pattern.compile("(\\S+)=[\"']?((?:.(?![\"']?\\s+(?:\\S+)=|\\s*\\/?[>\"']))+.)[\"']?")
                             .matcher("testabsd async width='100' height=100 src=\"213more\"")
                             .results()
                             .map(MatchResult::group)
                             .collect(Collectors.toMap(o -> o.split("=")[0],
                                                       o -> upd.escape(o.split("=")[1].replaceAll("^[\"`' ]+|[\"`' ]+$", ""))));

        System.out.println(matches.entrySet());
    }
    default long sqlNumber(String number) {
        if (number == null || number.isBlank()) return -1;
        return Long.parseLong(number.replaceAll("[^0-9]", ""));
    }
    default int INT(Number i) {
        return (int) Math.min(Integer.MAX_VALUE, i.longValue());
    }
    String[][] tanslate = {
            {"ÒÓÔÕÖØÓÔÖÓÓÓООØÒŌФ", "O"}
    };
    default String CONCAT(String... args) {
        return String.join(",", args).replaceAll("^,|,$", "");
    }
    default String toASCII(String in) {
        if (in == null) return "";
        return in.replaceAll("[ÒÓÔÕÖØÓÔÖÓÓÓООØÒŌФ]", "O")
                 .replaceAll("[òóôõöøóôöóóóооøòōф]", "o")
                 .replaceAll("[ÙÚÛÜüЧЦÜÙÚ]", "U")
                 .replaceAll("[ùúûüüчцüùú]", "u")
                 .replaceAll("[ÌÍÎÏİI]", "I")
                 .replaceAll("[ìíîïi̇iíîï]", "i")
                 .replaceAll("[ÈÉÊË£ÉÉÈЕĘЁÊË]", "E")
                 .replaceAll("[èéêë£ééèеęёêë]", "e")
                 .replaceAll("[ÀÁÂÃÄÅĄДАÄÃÃÁÀАДĀ]", "A")
                 .replaceAll("[àáâãäåąдаäããáàадā]", "a")
                 .replaceAll("[Ÿ¥У]", "Y")
                 .replaceAll("[ÿ¥у]", "y")
                 .replaceAll("[йñпли]", "n")
                 .replaceAll("[ЙÑПЛИ]", "N")
                 .replaceAll("[гяř]", "r")
                 .replaceAll("[ГЯŘ]", "R")
                 .replaceAll("[çсçčс©]", "c")
                 .replaceAll("[ÇСÇČС]", "C")
                 .replaceAll("[Æ]", "AE")
                 .replaceAll("[æ]", "ae")
                 .replaceAll("[ы]", "bl")
                 .replaceAll("[ы]", "bl")
                 .replaceAll("[ß]", "ss")
                 .replaceAll("[ВЬ]", "B")
                 .replaceAll("[вь]", "b")
                 .replaceAll("[Бб]", "6")
                 .replaceAll("[зЭ]", "3")
                 .replaceAll("[ш]", "w")
                 .replaceAll("[Ш]", "W")
                 .replaceAll("[жх]", "x")
                 .replaceAll("[ЖХ]", "X")
                 .replaceAll("[м]", "m")
                 .replaceAll("[М]", "M")
                 .replaceAll("[ż]", "z")
                 .replaceAll("[Ż]", "Z")
                 .replaceAll("[к]", "k")
                 .replaceAll("[К]", "K")
                 .replaceAll("[н]", "h")
                 .replaceAll("[H]", "H")
                 .replaceAll("[т]", "t")
                 .replaceAll("[Т]", "T")
                 .replaceAll("[р]", "p")
                 .replaceAll("[Р]", "P")
                 .replaceAll("[–]", "-")
                 .replaceAll("[¿]", "?")
                 .replaceAll("[¦]", ":")
                 .replaceAll("[¡]", "!")
                ;
    }
    //¤§Ä¼Ã¹¡?¦Сè❤ñàöÐâß:теяблю…оïç’îé,´
    default String quote(String in) {
        if (in == null) return null;
        return in.replaceAll("[´’`]|&#39;", "'");
    }
    default String escapeHeaderDescription(String in) {
        if (in == null) return "";
        return quote(toASCII(StringEscapeUtils.unescapeHtml4(in)))
                .replaceAll("[{\\[]", "(")
                .replaceAll("[}\\]]", ")")
                .replaceAll("\\s+", " ")
                .replaceAll("[;]", ",")
                .replaceAll("[\"]", "'");
    }
    default String escapeStrict(String in) {
        if (in == null) return "";
        return quote(StringEscapeUtils.unescapeHtml4(in))
                .replaceAll("[\\\\{\\[]", "(")
                .replaceAll("[/}\\]]", ")")
                .replaceAll("\\s+", " ")
                .replaceAll("[;:?&]", ",")
                .replaceAll("[\"]", "'");
    }
    default String escape(String in) {
        if (in == null) return "";
        return quote(toASCII(StringEscapeUtils.unescapeHtml4(in)))
                .replaceAll("[{\\[]", "(")
                .replaceAll("[}\\]]", ")")
                .replaceAll("\\s+", " ")
                .replaceAll("[;&]", ",")
                .replaceAll("[\"]", "'");
    }
    default boolean isNumeric(String str) {
        // null or empty
        if (str == null || str.length() == 0) {
            return false;
        }
        return str.chars().allMatch(Character::isDigit);
    }
    @SneakyThrows
    default LongCall readZip(SCVContext ctx, Consumer<Reader> reader) {
        return () -> {
            var totalLength = 0L;
            try (val bis = new BufferedInputStream(ctx.getUrl().openStream());
                 val zis = new ZipInputStream(bis)) {
                val ze = zis.getNextEntry();
                totalLength = ze.getSize();
                LOG.info("File: %s Size: %s Last Modified %s"
                                 .formatted(ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY)));
                try (val br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {

                    reader.accept(br);
                }
            }
            return totalLength;
        };
    }
    @SneakyThrows
    default long readZip4gMaxDirect(SCVContext ctx, Consumer<Reader> reader) {
        long totalLength;
        try (var bis = new BufferedInputStream(ctx.getUrl().openStream());
             var zis = new ZipInputStream(bis)) {
            //we will only use the first entry
            var ze = zis.getNextEntry();
            //sure this will be only one file..
            LOG.info("File: %s Size: %s Last Modified %s"
                             .formatted(ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY)));
            totalLength = ze.getSize();
            try (var read = new StringReader(new String(zis.readAllBytes()))) {
                reader.accept(read);
            }
        }
        return totalLength;
    }
    interface Sneak<T> {
        default T with() {
            try {
                return call();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException();
            }
        }
        T call() throws Exception;
    }
    @SneakyThrows
    static <T> T sneak(Sneak<T> t) {
        return t.with();
    }
}
