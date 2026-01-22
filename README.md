# Distributed Image Processing Pipeline using Apache Kafka

---

## Overview
This project implements a **Distributed Image Processing Pipeline** using **Apache Kafka** to perform real-time, parallel image transformations across multiple nodes.  
The **master node** accepts an uploaded image, divides it into tiles, and distributes each tile as a task message through Kafka.  
Each **worker node** processes its assigned tiles (e.g., grayscale conversion) and returns the processed results to the master node.  
Finally, the master reassembles all processed tiles into the complete image.

---

## System Architecture (4 Nodes)

### 🟩 Node 1 — Client & Master
- Built using **Flask** (web UI + REST endpoints).  
- Splits uploaded images into 512 × 512 pixel tiles.  
- Publishes each tile as a message to Kafka topic `tasks`.  
- Listens to topic `results` to receive processed tiles.  
- Reconstructs the final processed image and saves it in the `results/` folder.  

### 🟦 Node 2 — Kafka Broker
- Acts as the **message backbone** for the distributed system.  
- Hosts the following Kafka topics:
  - `tasks` → carries image tiles from master to workers  
  - `results` → returns processed tiles from workers to master  
  - `heartbeats` → (optional) tracks worker activity and health  
- Handles partitioning (2 partitions → 2 workers) for load balancing.  

### 🟨 Node 3 — Worker 1
- Subscribes to the Kafka topic `tasks` as part of the **worker consumer group**.  
- Receives assigned tiles from the broker.  
- Performs image transformations using **Pillow (PIL)** or **OpenCV** (e.g., grayscale).  
- Publishes processed image tiles to the `results` topic.  
- Optionally sends heartbeat messages to the `heartbeats` topic.  

### 🟧 Node 4 — Worker 2
- Identical to Worker 1 but runs **in parallel**, automatically handling a different Kafka partition.  
- Demonstrates **parallel task processing** and **load-balanced fault tolerance**.  
- Publishes processed tiles back to the `results` topic for the master to reassemble.  

---

## 🧰 Technologies Used
- **Apache Kafka** — Real-time message streaming and task distribution  
- **Flask (Python)** — Web framework for the UI and API  
- **Confluent Kafka Python API** — Kafka integration  
- **Pillow (PIL)** / **OpenCV** — Image processing libraries  
- **ZeroTier** — Virtual networking for distributed node communication  

---

## How to Run

### Step 1️⃣ — Start Kafka Broker (Node 2)
```bash
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties


kafka-topics.sh --create --topic tasks --partitions 2 --replication-factor 1 --bootstrap-server <broker-ip>:9092
kafka-topics.sh --create --topic results --partitions 2 --replication-factor 1 --bootstrap-server <broker-ip>:9092
kafka-topics.sh --create --topic heartbeats --partitions 1 --replication-factor 1 --bootstrap-server <broker-ip>:9092

python3 app.py

python3 worker1.py
python3 worker2.py
