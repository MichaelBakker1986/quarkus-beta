package nl.appmodel;

import io.quarkus.runtime.Startup;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.PostConstruct;
import javax.inject.Singleton;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
@Startup
@Singleton
public class Realtime {
    @PostConstruct
    private void init() {
        log.info("Hello");
    }
}
