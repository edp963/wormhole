package edp.wormhole.sparkx.swifts.custom.sensors.entry;

import java.util.List;

public class ZkDataEntry {
    private Integer version;

    private List<SchemaEntry> schemas;

    public void setSchemas(List<SchemaEntry> schemas) {
        this.schemas = schemas;
    }

    public List<SchemaEntry> getSchemas() {
        return schemas;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Integer getVersion() {
        return version;
    }
}
