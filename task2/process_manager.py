import multiprocessing as mp
import time

class ProcessManager:
    def __init__(self, num_processes, target_func, categories, results, use_cdp, cdp_endpoint):
        self.num_processes = num_processes
        self.target_func = target_func
        self.categories = categories
        self.results = results
        self.use_cdp = use_cdp
        self.cdp_endpoint = cdp_endpoint
        self.processes = []

    def start_processes(self):
        chunk_size = len(self.categories) // self.num_processes
        for i in range(self.num_processes):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i < self.num_processes - 1 else len(self.categories)
            subset = self.categories[start:end]
            p = mp.Process(target=self.target_func, args=(subset, self.results, self.use_cdp, self.cdp_endpoint))
            p.start()
            self.processes.append(p)

    def monitor(self):
        while any(p.is_alive() for p in self.processes):
            for i, p in enumerate(self.processes):
                if not p.is_alive() and p.exitcode != 0:
                    print(f"Process {i} failed. Restarting...")
                    subset = self.categories[(i * (len(self.categories) // self.num_processes)):((i + 1) * (len(self.categories) // self.num_processes))]
                    new_p = mp.Process(target=self.target_func, args=(subset, self.results, self.use_cdp, self.cdp_endpoint))
                    new_p.start()
                    self.processes[i] = new_p
            time.sleep(1)
        for p in self.processes:
            p.join()