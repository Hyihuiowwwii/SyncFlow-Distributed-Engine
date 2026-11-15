package com.syncflow.syncflow_engine;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MasterInventoryRepository extends MongoRepository<MasterInventory, String> {
    Optional<MasterInventory> findByRecordId(String recordId);
}
