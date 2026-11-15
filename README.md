# SyncFlow — Distributed Edge-to-Cloud Synchronization Engine

A fault-tolerant, multi-container distributed state synchronization engine built with **Spring Boot**, **Nginx**, **MongoDB (WiredTiger)**, and **Docker Compose**.

## Container Architecture
* **Docker 1 (`syncflow-security`)**: Nginx Reverse Proxy & WAF Ingress Gateway (Port `8080`) with rate-limiting and token validation.
* **Docker 2 (`syncflow-core` & `syncflow-edge`)**: Spring Boot JVM Core Engine implementing OS-level concurrency and an interactive Edge Client simulator.
* **Docker 3 (`syncflow-mongodb`)**: Persistent MongoDB 8.0 datastore backed by Docker volumes (`mongo-data`).

## Core OS & DBMS Algorithms
* **Producer-Consumer Bounded Buffer**: `ArrayBlockingQueue<SyncTask>(100)` for ingress backpressure control.
* **Fixed Worker Thread Pool**: `Executors.newFixedThreadPool(4)` mapped to container vCPUs.
* **Strict Two-Phase Locking (2PL)**: Per-record `ReentrantLock` with `tryLock` deadlock avoidance (Growing & Shrinking phases).
* **Write-Ahead Logging (WAL)**: ARIES-style append-only durability journal logged prior to WiredTiger persistence.
* **Offline Partition Tolerance**: Client-side RAM outbox queue with automatic replay upon network recovery.

## Quick Start
```bash
docker compose up -d --build
