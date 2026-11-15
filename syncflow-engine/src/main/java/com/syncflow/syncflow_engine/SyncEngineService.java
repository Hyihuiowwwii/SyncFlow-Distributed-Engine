package com.syncflow.syncflow_engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@Service
public class SyncEngineService {

    @Autowired
    private MasterInventoryRepository repository;

    
    private final BlockingQueue<SyncTask> taskQueue = new ArrayBlockingQueue<>(100);
    private final ExecutorService workerPool = Executors.newFixedThreadPool(4);
    private final ConcurrentHashMap<String, ReentrantLock> recordLocks = new ConcurrentHashMap<>();

    
    private final List<String> writeAheadLog = Collections.synchronizedList(new ArrayList<>());

    @PostConstruct
    public void init() {
        if (repository.count() == 0) {
            repository.save(new MasterInventory("maggie", 100, 1));
            repository.save(new MasterInventory("lays_chips", 50, 1));
        }

        for (int i = 0; i < 4; i++) {
            workerPool.submit(this::processQueue);
        }
    }

    private void processQueue() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                SyncTask task = taskQueue.take();
                executeStrict2PL(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public CompletableFuture<Map<String, Object>> commitTransaction(String recordId, int delta, long baseVersion) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
        SyncTask task = new SyncTask(recordId, delta, baseVersion, future);
        taskQueue.offer(task);
        return future;
    }

    
    private void executeStrict2PL(SyncTask task) {
        ReentrantLock lock = recordLocks.computeIfAbsent(task.recordId, k -> new ReentrantLock());
        Map<String, Object> response = new HashMap<>();

        try {
            
            boolean acquired = lock.tryLock(2, TimeUnit.SECONDS);
            if (!acquired) {
                response.put("status", "DEADLOCK_PREVENTED");
                response.put("message", "Lock acquisition timed out (Circular Wait aborted).");
                task.future.complete(response);
                return;
            }

            try {
                MasterInventory item = repository.findByRecordId(task.recordId)
                        .orElse(new MasterInventory(task.recordId, 100, task.baseVersion));

                int oldVal = item.getQuantity();
                int newVal = oldVal + task.delta;

                
                String walEntry = String.format("[WAL-LOG %s] TX_%s KEY=%s OLD=%d NEW=%d STATUS=PREPARED",
                        Instant.now().toString(), UUID.randomUUID().toString().substring(0, 8), task.recordId, oldVal, newVal);
                writeAheadLog.add(walEntry);

                
                item.setQuantity(newVal);
                item.setVersion(item.getVersion() + 1);
                MasterInventory saved = repository.save(item);

                
                writeAheadLog.add(String.format("[WAL-COMMIT] KEY=%s v%d COMMITTED TO WIREDTIGER", task.recordId, saved.getVersion()));

                response.put("status", "SUCCESS");
                response.put("wal", walEntry);
                response.put("data", saved);
                task.future.complete(response);

            } finally {
                
                lock.unlock();
            }

        } catch (Exception e) {
            response.put("status", "ERROR");
            response.put("error", e.getMessage());
            task.future.completeExceptionally(e);
        }
    }

    public List<String> getWalLogs() {
        int size = writeAheadLog.size();
        return writeAheadLog.subList(Math.max(0, size - 15), size);
    }

    private static class SyncTask {
        String recordId;
        int delta;
        long baseVersion;
        CompletableFuture<Map<String, Object>> future;

        SyncTask(String recordId, int delta, long baseVersion, CompletableFuture<Map<String, Object>> future) {
            this.recordId = recordId;
            this.delta = delta;
            this.baseVersion = baseVersion;
            this.future = future;
        }
    }
}