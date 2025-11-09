import base64
import io
import uuid
import json
import threading
import os
import time
import sqlite3
from flask import Flask, request, render_template, jsonify, send_file
from confluent_kafka import Producer, Consumer, KafkaError
from PIL import Image

# --- Configuration ---
# TODO: Update with your Broker IP
KAFKA_BROKER_IP = "172.25.187.130:9092"
KAFKA_TASK_TOPIC = "tasks"
KAFKA_RESULT_TOPIC = "results"
KAFKA_HEARTBEAT_TOPIC = "heartbeats"
TILE_WIDTH = 512
TILE_HEIGHT = 512
DB_PATH = "master.db"

# --- Global State ---
# Stores the last time we heard from each worker: { "worker-1": 1678886400.5, ... }
active_workers = {}

# --- Kafka Producer Setup ---
producer_conf = {'bootstrap.servers': KAFKA_BROKER_IP, 'client.id': 'master-producer'}
producer = Producer(producer_conf)

app = Flask(__name__)

# --- Database Helper Functions ---
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT,
            total_tiles INTEGER,
            tiles_processed INTEGER DEFAULT 0,
            original_width INTEGER,
            original_height INTEGER,
            final_image_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_tiles (
            job_id TEXT,
            tile_index INTEGER,
            tile_data TEXT,
            PRIMARY KEY (job_id, tile_index),
            FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
        )
    ''')
    conn.commit()
    conn.close()
    print("[DB] SQLite database initialized.")

# --- Image Tiling Logic ---
def tile_image(image_file, job_id):
    img = Image.open(image_file)
    img_width, img_height = img.size
    total_tiles_x = (img_width + TILE_WIDTH - 1) // TILE_WIDTH
    total_tiles_y = (img_height + TILE_HEIGHT - 1) // TILE_HEIGHT
    total_tiles = total_tiles_x * total_tiles_y
    
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO jobs (job_id, status, total_tiles, original_width, original_height) VALUES (?, ?, ?, ?, ?)",
                   (job_id, 'processing', total_tiles, img_width, img_height))
    conn.commit()
    conn.close()
    
    tile_index = 0
    for y in range(0, img_height, TILE_HEIGHT):
        for x in range(0, img_width, TILE_WIDTH):
            box = (x, y, min(x + TILE_WIDTH, img_width), min(y + TILE_HEIGHT, img_height))
            tile = img.crop(box)
            with io.BytesIO() as buffer:
                tile.save(buffer, format="PNG")
                tile_bytes = buffer.getvalue()
            tile_data_b64 = base64.b64encode(tile_bytes).decode('utf-8')
            message = {
                "job_id": job_id, "tile_index": tile_index,
                "total_tiles": total_tiles, "tile_data": tile_data_b64
            }
            producer.produce(KAFKA_TASK_TOPIC, key=job_id, value=json.dumps(message))
            tile_index += 1
    producer.flush()
    return total_tiles

# --- Image Re-assembly Logic ---
def reassemble_image(job_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
    job_info = cursor.fetchone()
    if not job_info or job_info['status'] == 'complete':
        conn.close()
        return
    print(f"[Master] Re-assembling image for job {job_id}...")
    cursor.execute("SELECT tile_index, tile_data FROM processed_tiles WHERE job_id = ? ORDER BY tile_index ASC", (job_id,))
    tiles = cursor.fetchall()
    final_image = Image.new("L", (job_info['original_width'], job_info['original_height']))
    for tile_row in tiles:
        tile_bytes = base64.b64decode(tile_row['tile_data'])
        tile_img = Image.open(io.BytesIO(tile_bytes))
        tiles_per_row = (job_info['original_width'] + TILE_WIDTH - 1) // TILE_WIDTH
        tile_x_idx = tile_row['tile_index'] % tiles_per_row
        tile_y_idx = tile_row['tile_index'] // tiles_per_row
        final_image.paste(tile_img, (tile_x_idx * TILE_WIDTH, tile_y_idx * TILE_HEIGHT))
    output_path = os.path.join("results", f"{job_id}.png")
    final_image.save(output_path)
    cursor.execute("UPDATE jobs SET status = 'complete', final_image_path = ? WHERE job_id = ?", (output_path, job_id))
    conn.commit()
    conn.close()
    print(f"[Master] Job {job_id} complete!")

# --- THREAD 1: Results Consumer ---
def start_results_consumer():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_IP,
        'group.id': 'master-results-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([KAFKA_RESULT_TOPIC])
    print("[Master] Results consumer started.")
    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error(): continue
        try:
            result = json.loads(msg.value().decode('utf-8'))
            job_id, tile_index = result["job_id"], result["tile_index"]
            conn = get_db()
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO processed_tiles (job_id, tile_index, tile_data) VALUES (?, ?, ?)",
                           (job_id, tile_index, result["tile_data"]))
            if cursor.rowcount > 0:
                 cursor.execute("UPDATE jobs SET tiles_processed = tiles_processed + 1 WHERE job_id = ?", (job_id,))
            conn.commit()
            cursor.execute("SELECT total_tiles, tiles_processed, status FROM jobs WHERE job_id = ?", (job_id,))
            job_row = cursor.fetchone()
            conn.close()
            if job_row and job_row['tiles_processed'] >= job_row['total_tiles'] and job_row['status'] == 'processing':
                 reassemble_image(job_id)
        except Exception as e: print(f"[Master] Result error: {e}")

# --- THREAD 2: Heartbeat Consumer (NEW!) ---
def start_heartbeat_consumer():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_IP,
        'group.id': 'master-heartbeat-monitor',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([KAFKA_HEARTBEAT_TOPIC])
    print("[Master] Heartbeat monitor started.")
    while True:
        msg = consumer.poll(1.0)
        if not msg or msg.error(): continue
        try:
            # Expected format: {"worker_id": "worker-1", "timestamp": 1234567890}
            data = json.loads(msg.value().decode('utf-8'))
            worker_id = data.get("worker_id")
            if worker_id:
                # Update the last time we heard from this worker
                active_workers[worker_id] = time.time()
        except Exception as e: print(f"[Master] Heartbeat error: {e}")

# --- Flask Web Routes ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_image():
    if 'image' not in request.files or request.files['image'].filename == '': return "No file", 400
    job_id = str(uuid.uuid4())
    print(f"[Master] New job: {job_id}")
    try:
        total = tile_image(request.files['image'].stream, job_id)
        return jsonify({"message": "Started", "job_id": job_id, "total_tiles_sent": total})
    except Exception as e: return f"Error: {e}", 500

@app.route('/status/<job_id>')
def get_job_status(job_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT status, total_tiles, tiles_processed FROM jobs WHERE job_id = ?", (job_id,))
    job = cursor.fetchone()
    conn.close()
    return jsonify(dict(job)) if job else (jsonify({"error": "Not found"}), 404)

@app.route('/result/<job_id>')
def get_result_image(job_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT status, final_image_path FROM jobs WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()
    conn.close()
    return send_file(row['final_image_path'], mimetype='image/png') if row and row['status'] == 'complete' else (jsonify({"error": "Not ready"}), 404)

# --- NEW ENDPOINT: Worker Status ---
@app.route('/workers')
def get_workers():
    # 1. Clean up old workers who haven't sent a heartbeat in 15 seconds
    current_time = time.time()
    dead_workers = [w_id for w_id, last_seen in active_workers.items() if current_time - last_seen > 15]
    for w_id in dead_workers:
        del active_workers[w_id]
        
    # 2. Return the list of active workers
    return jsonify({
        "active_count": len(active_workers),
        "workers": list(active_workers.keys())
    })

# --- Main Execution ---
if __name__ == '__main__':
    if not os.path.exists('results'): os.makedirs('results')
    init_db()
    # Start BOTH background threads
    threading.Thread(target=start_results_consumer, daemon=True).start()
    threading.Thread(target=start_heartbeat_consumer, daemon=True).start()
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
