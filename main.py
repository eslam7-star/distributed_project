from mpi4py import MPI

# Get rank and size from the MPI environment
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Change these values as needed
num_crawlers = 2
num_indexers = 1

# Safety check
if size < 1 + num_crawlers + num_indexers:
    if rank == 0:
        print(f"Error: Not enough processes. Expected at least {1 + num_crawlers + num_indexers}, got {size}.")
    exit()

# Dispatcher based on rank
if rank == 0:
    from master_node import master_process
    master_process()

elif 1 <= rank <= num_crawlers:
    from crawler_node import crawler_process
    crawler_process()

elif num_crawlers < rank <= num_crawlers + num_indexers:
    from indexer_node import indexer_process
    indexer_process()

else:
    print(f"Rank {rank} is unassigned. No role specified.")