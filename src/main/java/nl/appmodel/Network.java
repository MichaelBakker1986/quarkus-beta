package nl.appmodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
@AllArgsConstructor
@Getter
public enum Network {
    PornHub(PornhubDTO.class),
    xtube(XtubeDTO.class),
    YouPorn(YoupornDTO.class),
    Xvideos(XVideoDTO.class);
    Class<? extends TagAndId> dto;
    public String tableName() {
        return "prosite." + name().toLowerCase();
    }
}
