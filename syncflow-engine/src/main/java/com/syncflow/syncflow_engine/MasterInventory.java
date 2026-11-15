package com.syncflow.syncflow_engine;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "master_inventory")
public class MasterInventory {

    @Id
    private String id;
    private String recordId;
    private int quantity;
    private long version;

    public MasterInventory() {}

    public MasterInventory(String recordId, int quantity, long version) {
        this.recordId = recordId;
        this.quantity = quantity;
        this.version = version;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getRecordId() { return recordId; }
    public void setRecordId(String recordId) { this.recordId = recordId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
