from mpi4py import MPI
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def master_process():
    """
    Main process for the master node.
    Handles task distribution, crawler/indexer coordination, and basic monitoring.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    # Setup nodes
    crawler_nodes = size - 2  # Assuming 1 master, 1 indexer
    indexer_nodes = 1
    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))
    active_indexer_nodes = list(range(1 + crawler_nodes, size))

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    # Initial Seed URLs (can be replaced with file/DB input)
    seed_urls = ["http://example.com", "http://example.org"]
    urls_to_crawl_queue = seed_urls.copy()

    task_count = 0
    crawler_tasks_assigned = 0
    indexer_assignment_round = 0  # For simple round-robin indexing

    while urls_to_crawl_queue or crawler_tasks_assigned > 0:
        # Check for messages from crawlers
        if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
            message_source = status.Get_source()
            message_tag = status.Get_tag()
            message_data = comm.recv(source=message_source, tag=message_tag)

            if message_tag == 1:  # Crawler sends back extracted links + content
                crawler_tasks_assigned -= 1
                extracted = message_data  # Should be a dict: {'urls': [...], 'text': "..."}
                new_urls = extracted.get("urls", [])
                page_text = extracted.get("text", "")
                source_url = extracted.get("url", "")

                # Add new URLs to crawl queue
                if new_urls:
                    urls_to_crawl_queue.extend(new_urls)

                # Assign content to indexer
                if page_text:
                    assigned_indexer = active_indexer_nodes[indexer_assignment_round % len(active_indexer_nodes)]
                    indexer_assignment_round += 1
                    index_data = {
                        "url": source_url,
                        "content": page_text
                    }
                    comm.send(index_data, dest=assigned_indexer, tag=2)  # Tag 2 for indexing task
                    logging.info(f"Master sent index data for {source_url} to Indexer {assigned_indexer}")

                logging.info(f"Master received data from Crawler {message_source}, Queue: {len(urls_to_crawl_queue)}, Tasks: {crawler_tasks_assigned}")

            elif message_tag == 99:
                logging.info(f"Crawler {message_source} heartbeat: {message_data}")

            elif message_tag == 999:
                logging.error(f"Crawler {message_source} error: {message_data}")
                crawler_tasks_assigned -= 1

        # Assign crawl tasks
        while urls_to_crawl_queue and crawler_tasks_assigned < crawler_nodes:
            url_to_crawl = urls_to_crawl_queue.pop(0)
            assigned_crawler = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]
            task_id = task_count
            task_count += 1

            comm.send(url_to_crawl, dest=assigned_crawler, tag=0)
            crawler_tasks_assigned += 1
            logging.info(f"Assigned task {task_id} - Crawl {url_to_crawl} to Crawler {assigned_crawler}")

        time.sleep(0.5)  # Reduce CPU usage in loop

    logging.info("Master finished distributing tasks. Waiting for remaining crawlers to complete.")
    print("Master Node Finished.")

if __name__ == '__main__':
    master_process()