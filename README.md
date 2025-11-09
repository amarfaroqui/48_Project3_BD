# Distributed Image Processing Pipeline using Apache Kafka

### Team 48 - Big Data Project (Project 3)
**Course:** Big Data  
**Submission Date:** November 9, 2025  

---

## Overview
This project implements a **Distributed Image Processing Pipeline** using **Apache Kafka**, where large images are divided into smaller tiles, processed concurrently by worker nodes, and reconstructed into a final processed image by the master node.

---

## System Architecture
- **Node 1 (Client & Master):** Flask web interface for image upload, tiling logic, and Kafka producer to distribute tiles.  
- **Node 2 (Kafka Broker):** Handles communication between master and workers through topics (`tasks`, `results`, `heartbeats`).  
- **Node 3 & Node 4 (Workers):** Kafka consumers that perform image transformations (e.g., grayscale conversion) and send results back.  

---

## Technologies Used
- **Apache Kafka** – Message streaming backbone  
- **Python (Flask, Confluent Kafka, Pillow)** – Web app and image processing  
- **ZeroTier** – Virtual networking across distributed nodes  

---

## How to Run
1. Start **Zookeeper** and **Kafka Broker** on Node 2.  
2. Run **Master Node** (app.py) on Node 1:  
   ```bash
   python3 app.py

