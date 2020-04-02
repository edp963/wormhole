package edp.wormhole.sparkx.swifts.custom.sensors.entry;

public class SchemaEntry {
    private String name;

    private Integer type;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Integer getType() {
        return type;
    }

    public SchemaEntry() {}

    public SchemaEntry(String name, Integer type) {
        this.name = name;
        this.type = type;
    }


}
