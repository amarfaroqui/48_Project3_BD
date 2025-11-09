import base64
import io
import json
import threading
import time
import sys
from confluent_kafka import Consumer, Producer, KafkaError
from PIL import Image

# --- Configuration ---
# TODO: CRITICAL! Update this with Person 2's Kafka IP address
KAFKA_BROKER_IP = "172.25.187.130:9092"  # Example: "192.168.1.102:9092"

 
WORKER_ID = "worker-2" 

# TODO: CRITICAL! This MUST be the same for Person 3 and Person 4
CONSUMER_GROUP_ID = "image-processors" 

KAFKA_TASK_TOPIC = "tasks"
KAFKA_RESULT_TOPIC = "results"
KAFKA_HEARTBEAT_TOPIC = "heartbeats"


# --- Kafka Producer Setup ---
# We need a producer to send results and heartbeats
producer_conf = {'bootstrap.servers': KAFKA_BROKER_IP}
producer = Producer(producer_conf)


# --- Kafka Consumer Setup ---
# This is how we get tasks
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER_IP,
    'group.id': CONSUMER_GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start from the beginning if we're new
}
consumer = Consumer(consumer_conf)


def process_image(tile_data_b64):
    """
    Decodes a base64 image, converts it to grayscale,
    and re-encodes it as base64.
    """
    try:
        # 1. Decode from base64 string to raw bytes
        tile_bytes = base64.b64decode(tile_data_b64)
        
        # 2. Load bytes into an in-memory image
        img_buffer = io.BytesIO(tile_bytes)
        img = Image.open(img_buffer)
        
        # 3. Process the image (convert to grayscale)
        grayscale_img = img.convert("L")
        
        # 4. Save processed image back to an in-memory buffer
        with io.BytesIO() as output_buffer:
            grayscale_img.save(output_buffer, format="PNG") # Save as PNG
            processed_bytes = output_buffer.getvalue()
            
        # 5. Re-encode raw bytes back to base64 string
        processed_data_b64 = base64.b64encode(processed_bytes).decode('utf-8')
        
        return processed_data_b64
        
    except Exception as e:
        print(f"[{WORKER_ID}] Error processing image: {e}")
        return None


def send_heartbeat():
    """
    Runs in a separate thread, sending a heartbeat every 5 seconds.
    """
    while True:
        try:
            heartbeat_msg = {
                "worker_id": WORKER_ID,
                "timestamp": int(time.time())
            }
            # Send the heartbeat message
            producer.produce(
                KAFKA_HEARTBEAT_TOPIC,
                key=WORKER_ID,
                value=json.dumps(heartbeat_msg) # Send as a JSON string
            )
            producer.poll(0) # Non-blocking flush
            # print(f"[{WORKER_ID}] Sent heartbeat.")
        except Exception as e:
            print(f"[{WORKER_ID}] Error sending heartbeat: {e}")
        
        time.sleep(5) # Wait 5 seconds


def main_task_loop():
    """
    Main loop for the consumer. Polls for tasks, processes them,
    and sends the results.
    """
    print(f"--- Worker {WORKER_ID} starting ---")
    print(f"Connecting to Kafka at {KAFKA_BROKER_IP}")
    print(f"Joining consumer group '{CONSUMER_GROUP_ID}'")
    print(f"Listening for tasks on topic '{KAFKA_TASK_TOPIC}'...")

    consumer.subscribe([KAFKA_TASK_TOPIC])

    try:
        while True:
            # Poll for a new message for up to 1 second
            msg = consumer.poll(1.0)
            
            if msg is None:
                # No message received
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(f"[{WORKER_ID}] Consumer error: {msg.error()}")
                    break

            # --- Message received! ---
            
            try:
                # 1. Deserialize the message value (it's a JSON string)
                task = json.loads(msg.value().decode('utf-8'))
                
                job_id = task['job_id']
                tile_index = task['tile_index']
                
                print(f"[{WORKER_ID}] Received task: Job {job_id}, Tile {tile_index}")

                # 2. Process the image
                processed_tile_data = process_image(task['tile_data'])
                
                if processed_tile_data:
                    # 3. Create the result message (same contract as before)
                    result_message = {
                        "job_id": job_id,
                        "tile_index": tile_index,
                        "total_tiles": task['total_tiles'],
                        "tile_data": processed_tile_data # Now grayscale!
                    }
                    
                    # 4. Produce the result to the 'results' topic
                    producer.produce(
                        KAFKA_RESULT_TOPIC,
                        key=job_id,
                        value=json.dumps(result_message) # Send as JSON string
                    )
                    producer.poll(0) # Non-blocking flush
                    print(f"[{WORKER_ID}] Finished & Sent: Job {job_id}, Tile {tile_index}")

            except json.JSONDecodeError:
                print(f"[{WORKER_ID}] Received non-JSON message: {msg.value()}")
            except Exception as e:
                print(f"[{WORKER_ID}] Error in main loop: {e}")

    except KeyboardInterrupt:
        print(f"[{WORKER_ID}] Shutting down...")
    finally:
        # Clean up
        consumer.close()
        print(f"[{WORKER_ID}] Worker stopped.")


# --- Main Execution ---
if __name__ == "__main__":
    # Start the heartbeat thread in the background
    # 'daemon=True' means the thread will exit when the main program exits
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    # Start the main task processing loop
    main_task_loop()
