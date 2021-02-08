package nl.appmodel;

import io.quarkus.runtime.Startup;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
@Slf4j
@Startup
@ApplicationScoped
public class Realtime {
    @PostConstruct
    private void init() {
        log.info("Hello");
    }
}
