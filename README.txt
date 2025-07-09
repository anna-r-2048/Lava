Overview
--------
The Lava Network Provider Pairing System is a Python-based solution designed to match service providers with consumer queries based on a specified policy. It processes queries concurrently, filters providers by location, features, and stake, ranks them using a normalized scoring mechanism, and returns the top 5 providers per query. The system is optimized for performance, supports asynchronous task completion via callbacks, and ensures robustness with error handling and task ID management.

Concurrency
-----------
For the Lava Network Provider Pairing System, where get_pairing_list involves potentially CPU-intensive operations (filtering and scoring large provider lists), using ProcessPoolExecutor in the custom workers pool ensures true parallelism by leveraging multiple processes, bypassing the GIL. This is particularly beneficial for handling a high volume of concurrent consumer queries efficiently.

Interaction Diagram
-------------------

Client -> PairingSystem.process_query(input_providers, policy, callback)
  |
  v
PairingSystem -> CustomWorkerPool.submit(get_pairing_list, args, callback)
  |
  v
ProcessPoolExecutor -> get_pairing_list(providers, policy, max_stake, max_extra_features)
  |        |
  |        v
  |     filter_providers(providers, policy)
  |        |
  |        v
  |     rank_providers(filtered_providers, policy, max_stake, max_extra_features)
  |
  v
CustomWorkerPool -> _handle_callback(task_id, result) -> Callback(task_id, result)
  |
  v
Client <- get_query_result(task_id) (optional, if no callback)



Assumtions:
----------
- Based on Location Filter, the provider with different location doesn't match the customer policy requirements.
- The presence of max_stake and max_extra_features  for calculating normalization values.

Possible Improvements
---------------------
Using DB for  saving providers data in the sorted way (according to different fields) so we will not need to send this data over the queues. 
