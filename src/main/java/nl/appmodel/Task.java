package nl.appmodel;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import javax.persistence.Entity;
import java.time.Instant;
@Entity
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class Task extends PanacheEntity {
    public Instant createdAt;
    public Task() {
        createdAt = Instant.now();
    }
    public Task(Instant time) {
        this.createdAt = time;
    }
}
