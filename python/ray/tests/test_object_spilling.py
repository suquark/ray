import json
import random
import time

import numpy as np
import pytest
import ray


def test_spill_objects_manually(shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _internal_config=json.dumps({
            "object_store_full_max_retries": 0,
            "num_io_workers": 4,
        }))
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []
    pinned_objects = set()
    spilled_objects = set()

    # Create objects of more than 200 MiB.
    for _ in range(25):
        ref = None
        while ref is None:
            try:
                ref = ray.put(arr)
                replay_buffer.append(ref)
                pinned_objects.add(ref)
            except ray.exceptions.ObjectStoreFullError:
                ref_to_spill = pinned_objects.pop()
                ray.experimental.force_spill_objects([ref_to_spill])
                spilled_objects.add(ref_to_spill)

    # Spill 2 more objects so we will always have enough space for
    # restoring objects back.
    refs_to_spill = (pinned_objects.pop(), pinned_objects.pop())
    ray.experimental.force_spill_objects(refs_to_spill)
    spilled_objects.update(refs_to_spill)

    # randomly sample objects
    for _ in range(100):
        ref = random.choice(replay_buffer)
        if ref in spilled_objects:
            ray.experimental.force_restore_spilled_objects([ref])
        sample = ray.get(ref)
        assert np.array_equal(sample, arr)


@pytest.mark.skip(reason="have not been fully implemented")
def test_spill_objects_automatically(shutdown_only):
    # Limit our object store to 75 MiB of memory.
    ray.init(
        object_store_memory=75 * 1024 * 1024,
        _internal_config=json.dumps({
            "num_io_workers": 4,
            "object_store_full_max_retries": 2,
            "object_store_full_initial_delay_ms": 10,
            "auto_object_spilling": True,
        }))
    arr = np.random.rand(1024 * 1024)  # 8 MB data
    replay_buffer = []

    # Wait raylet for starting an IO worker.
    time.sleep(1)

    # Create objects of more than 800 MiB.
    for _ in range(100):
        ref = None
        while ref is None:
            ref = ray.put(arr)
            replay_buffer.append(ref)

    print("-----------------------------------")

    # randomly sample objects
    for _ in range(1000):
        ref = random.choice(replay_buffer)
        sample = ray.get(ref, timeout=0)
        assert np.array_equal(sample, arr)