package nl.appmodel;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode.Include;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
@Data
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PornhubDTO implements TagAndId {
    @Include long pro;
    String tag;
}
