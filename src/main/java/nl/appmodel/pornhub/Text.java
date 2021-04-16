package nl.appmodel.pornhub;

import lombok.AllArgsConstructor;
import lombok.extern.java.Log;
import lombok.val;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
@AllArgsConstructor
@Log
public class Text {
    String story, err;
    public void print() {
        try {
            Matcher m = Pattern.compile("at line ([0-9]+)").matcher(this.err);   // the pattern to search for
            m.find();
            var lines = Integer.parseInt(m.group(1));
            val split = this.story.split("\n");
            log.info(split[Math.max(0, lines - 1)] + "\n" + split[lines] + '\n' + split[Math.min(split.length, lines + 1)]);
        } catch (Exception e) {
            log.warning(story);
            log.warning(err);
            log.warning("Error" + e);
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
                log.warning(story);
                log.warning(err);
                log.warning("Error" + e);
            }
        }
    }
}
