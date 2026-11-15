package com.syncflow.syncflow_engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/sync")
@CrossOrigin(origins = "*")
public class SyncController {

    @Autowired
    private MasterInventoryRepository repository;

    @Autowired
    private SyncEngineService service;

    @GetMapping("/items")
    public List<MasterInventory> getItems() {
        return repository.findAll();
    }

    @GetMapping("/wal")
    public List<String> getWal() {
        return service.getWalLogs();
    }

    @PostMapping("/commit")
    public ResponseEntity<?> commit(@RequestBody Map<String, Object> payload) {
        String recordId = (String) payload.getOrDefault("recordId", "maggie");
        int delta = Integer.parseInt(payload.getOrDefault("delta", "-5").toString());
        long baseVersion = Long.parseLong(payload.getOrDefault("baseVersion", "1").toString());

        try {
            Map<String, Object> res = service.commitTransaction(recordId, delta, baseVersion).get(5, java.util.concurrent.TimeUnit.SECONDS);
            return ResponseEntity.ok(res);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}