import redis
import time
import logging

logging.basicConfig(level=logging.INFO)
r = redis.Redis(host='redis', port=6379)

def main():
    while True:
        try:
            r.set(f"heartbeat:master", str(time.time()))
            logging.info("Master is running...")
            time.sleep(10)
        except Exception as e:
            logging.error(f"Master error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()

    