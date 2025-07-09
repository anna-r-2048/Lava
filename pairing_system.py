import concurrent.futures
import multiprocessing
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
import time

# Data structures as defined in the assignment
@dataclass
class Provider:
    address: str
    stake: int
    location: str
    features: List[str]

@dataclass
class ConsumerPolicy:
    required_location: str
    required_features: List[str]
    min_stake: int

@dataclass
class PairingScore:
    provider: Provider
    score: float
    components: Dict[str, float]

class CustomWorkerPool:
    """
    Custom worker pool using ProcessPoolExecutor for true parallelism, bypassing GIL.
    Manages a fixed number of processes to handle get_pairing_list tasks concurrently.
    Uses Future objects for result collection and supports callbacks for task completion.
    Resets task counter when reaching MAX_TASK_ID if task_id=0 is not pending.
    """
    MAX_TASK_ID = 2**32 - 1  # Maximum task ID before reset (4,294,967,295)

    def __init__(self, num_workers: int = 4):
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)
        self.futures = {}  # Store Future objects with task_id
        self.callbacks = {}  # Store callbacks with task_id
        self.task_counter = 0  # Local counter for task IDs
        self.num_workers = num_workers

    def submit(self, task, *args, callback: Optional[Callable[[int, List[Provider]], None]] = None) -> int:
        """
        Submits a task to the process pool and returns a task ID.
        Optionally registers a callback to be called when the task completes.
        Resets task_counter if it reaches MAX_TASK_ID and task_id=0 is not pending.
        """
        try:
            if self.task_counter >= self.MAX_TASK_ID:
                if 0 in self.futures:
                    raise RuntimeError("Cannot reset task_counter: task_id=0 is still pending")
                self.task_counter = 0  # Reset counter if task_id=0 is not active

            task_id = self.task_counter
            self.task_counter += 1
            future = self.executor.submit(task, *args)
            self.futures[task_id] = future
            if callback is not None:
                self.callbacks[task_id] = callback
                future.add_done_callback(lambda f: self._handle_callback(task_id, f))
            return task_id
        except Exception as e:
            print(f"Failed to submit task: {e}")
            raise

    def _handle_callback(self, task_id: int, future: concurrent.futures.Future):
        """
        Executes the callback for a completed task, if registered.
        """
        if task_id not in self.callbacks:
            return
        callback = self.callbacks[task_id]
        try:
            result = future.result()
            callback(task_id, result)
        except Exception as e:
            print(f"Callback for task {task_id} failed: {e}")
        finally:
            del self.callbacks[task_id]  # Clean up

    def get_result(self, task_id: int) -> Optional[List[Provider]]:
        """
        Retrieves the result for a specific task ID, waiting if necessary.
        """
        try:
            if task_id not in self.futures:
                return None
            future = self.futures[task_id]
            result = future.result()  # Blocks until result is ready
            del self.futures[task_id]  # Clean up
            return result
        except Exception as e:
            print(f"Failed to retrieve result for task {task_id}: {e}")
            return None

    def shutdown(self):
        """
        Shutdown the process pool.
        """
        try:
            self.executor.shutdown(wait=True)
        except Exception as e:
            print(f"Failed to shutdown worker pool: {e}")

class PairingSystem:
    def __init__(
        self,
        num_workers: int = multiprocessing.cpu_count(),
        max_stake: int = 1_000_000,
        max_extra_features: int = 10,
    ):
        """
        Initialize PairingSystem with a process-based worker pool and scoring parameters.
        Worker pool is reused across queries for efficiency.
        Args:
            num_workers: Number of processes in the worker pool.
            max_stake: Maximum stake for score normalization.
            max_extra_features: Maximum extra features for score normalization.
        """
        self.worker_pool = CustomWorkerPool(num_workers)
        self.max_stake = max_stake
        self.max_extra_features = max_extra_features

    def __del__(self):
        """
        Ensure worker pool is shut down when PairingSystem is destroyed.
        """
        self.worker_pool.shutdown()

    @staticmethod
    def filter_providers(providers: List[Provider], policy: ConsumerPolicy) -> List[Provider]:
        """
        Filters providers based on location, features, and stake requirements.
        Optimized with set-based feature lookup and single stake check with early termination.
        Assumes providers are pre-sorted by stake in descending order.
        """
        required_features_set = set(policy.required_features)  # Convert to set for O(1) lookup
        
        filtered = []
        for provider in providers:
            # Single stake check with early termination for sorted providers
            if provider.stake < policy.min_stake:
                break
            
            # LocationFilter: Check if provider location matches or is empty (wildcard)
            location_match = not policy.required_location or provider.location == policy.required_location
            
            # FeatureFilter: Use set for O(1) lookup
            feature_match = required_features_set.issubset(set(provider.features))
            
            if location_match and feature_match:
                filtered.append(provider)
        
        return filtered

    @staticmethod
    def rank_providers(
        providers: List[Provider],
        policy: ConsumerPolicy,
        max_stake: int,
        max_extra_features: int,
    ) -> List[PairingScore]:
        """
        Assigns normalized scores (0-1) to providers based on stake, features, and location.
        Optimized with simplified feature scoring and no caching (per query policy uniqueness).
        """
        required_features_set = set(policy.required_features)  # Convert to set for O(1) lookup

        def calculate_score(provider: Provider) -> PairingScore:
            components = {}
            
            # StakeScore: Normalize based on max stake
            components['stake'] = min(provider.stake / max_stake, 1.0)
            
            # FeatureScore: Use length difference for extra features
            extra_features = len(provider.features) - len(required_features_set)
            components['features'] = min(extra_features / max_extra_features, 1.0)
            
            # LocationScore: 1.0 for matching, 0.5 for non-matching, 0.0 if no location
            components['location'] = (
                1.0 if provider.location == policy.required_location 
                else 0.5 if provider.location else 0.0
            )
            
            # Weighted average of components (equal weights)
            total_score = sum(components.values()) / len(components)
            
            return PairingScore(provider=provider, score=total_score, components=components)

        scores = [calculate_score(p) for p in providers]
        return sorted(scores, key=lambda x: x.score, reverse=True)

    @staticmethod
    def get_pairing_list(
        providers: List[Provider],
        policy: ConsumerPolicy,
        max_stake: int,
        max_extra_features: int,
    ) -> List[Provider]:
        """
        Processes a single query: filters providers, ranks them, and returns the top 5.
        Static method to avoid pickling the PairingSystem instance.
        """
        if not providers or not policy:
            return []
        
        # Step 1: Filter providers
        filtered_providers = PairingSystem.filter_providers(providers, policy)
        
        if not filtered_providers:
            return []
        
        # Step 2: Rank providers
        ranked_scores = PairingSystem.rank_providers(
            filtered_providers, policy, max_stake, max_extra_features
        )
        
        # Step 3: Select top 5 providers
        return [score.provider for score in ranked_scores[:5]]

    def process_query(
        self,
        input_providers: List[Provider],
        policy: ConsumerPolicy,
        callback: Optional[Callable[[int, List[Provider]], None]] = None,
    ) -> int:
        """
        Submits a query to the process pool and returns a task ID for result retrieval.
        Pre-sorts providers by stake to optimize filtering.
        Uses input_providers to avoid shadowing outer scope variable.
        Args:
            input_providers: List of providers to process.
            policy: Consumer policy for filtering and ranking.
            callback: Optional function to call when the task completes, taking task_id and result.
        """
        try:
            if not isinstance(input_providers, list) or not isinstance(policy, ConsumerPolicy):
                raise ValueError("Invalid input: input_providers must be a list, policy must be ConsumerPolicy")
            # Pre-sort providers by stake in descending order
            sorted_providers = sorted(input_providers, key=lambda x: x.stake, reverse=True)
            return self.worker_pool.submit(
                PairingSystem.get_pairing_list,
                sorted_providers,
                policy,
                self.max_stake,
                self.max_extra_features,
                callback=callback,
            )
        except Exception as e:
            print(f"Failed to process query: {e}")
            raise

    def get_query_result(self, task_id: int) -> List[Provider]:
        """
        Retrieves the result of a query by task ID.
        """
        return self.worker_pool.get_result(task_id)

# Example callback function
def example_callback(task_id: int, result: List[Provider]) -> None:
    """
    Example callback to demonstrate task completion handling.
    Prints the task ID and provider details.
    """
    print(f"Callback triggered for Task {task_id}: {len(result)} providers returned")
    for provider in result:
        print(f"  Provider: {provider.address}, Stake: {provider.stake}, Location: {provider.location}")

# Faulty callback for testing error handling
def faulty_callback(task_id: int, result: List[Provider]) -> None:
    """
    Callback that raises an exception to test error handling.
    """
    raise ValueError(f"Faulty callback for task {task_id}")

# Example usage and testing
if __name__ == "__main__":
    # Ensure spawn method for Windows compatibility
    multiprocessing.set_start_method("spawn", force=True)
    
    # Sample providers
    providers = [
        Provider("addr1", 500000, "US", ["rpc", "websocket"]),
        Provider("addr2", 200000, "EU", ["rpc"]),
        Provider("addr3", 800000, "US", ["rpc", "websocket", "extra"]),
        Provider("addr4", 10000, "US", ["rpc"]),
        Provider("addr5", 600000, "", ["rpc", "websocket", "extra", "premium"])
    ]
    
    # Sample policy
    policy = ConsumerPolicy(
        required_location="US",
        required_features=["rpc"],
        min_stake=100000
    )
    
    # Create pairing system
    system = PairingSystem(max_stake=1_000_000, max_extra_features=10)
    
    print("=== Test 1: Standard Queries with Callback ===")
    task_ids = []
    for _ in range(3):  # Submit 3 queries with callback only
        task_id = system.process_query(providers, policy, callback=example_callback)
        task_ids.append(task_id)
    
    # Wait for tasks to complete (callbacks handle output)
    time.sleep(1)  # Brief sleep to allow callbacks to print
    
    print("\n=== Test 2: Synchronous Retrieval without Callback ===")
    task_id = system.process_query(providers, policy)  # No callback
    result = system.get_query_result(task_id)
    print(f"Query {task_id} Results:")
    for provider in result:
        print(f"  Provider: {provider.address}, Stake: {provider.stake}, Location: {provider.location}")
    
    print("\n=== Test 3: Empty Providers List ===")
    task_id = system.process_query([], policy, callback=example_callback)
    time.sleep(0.5)  # Allow callback to print
    
    print("\n=== Test 4: No Matching Providers ===")
    strict_policy = ConsumerPolicy(
        required_location="US",
        required_features=["rpc"],
        min_stake=1_000_000
    )
    task_id = system.process_query(providers, strict_policy, callback=example_callback)
    time.sleep(0.5)  # Allow callback to print
    
    print("\n=== Test 5: Task Counter Reset ===")
    system.worker_pool.task_counter = system.worker_pool.MAX_TASK_ID - 1
    task_id1 = system.process_query(providers, policy, callback=example_callback)
    result1 = system.get_query_result(task_id1)  # Clear task to allow reset
    task_id2 = system.process_query(providers, policy, callback=example_callback)
    print(f"Task ID 1: {task_id1}, Task ID 2: {task_id2} (should be 0 after reset)")
    time.sleep(0.5)  # Allow callback to print
    
    print("\n=== Test 6: Faulty Callback ===")
    task_id = system.process_query(providers, policy, callback=faulty_callback)
    time.sleep(0.5)  # Allow callback to fail gracefully