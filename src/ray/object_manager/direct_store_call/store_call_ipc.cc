#include "ray/object_manager/direct_store_call/store_call_ipc.h"

namespace ray {

ObjectStoreCallIPC::ObjectStoreCallIPC(std::string store_socket_name) {
  RAY_CHECK_OK(store_client_.Connect(store_socket_name));
}

Status ObjectStoreCallIPC::MarkObjectAsFailed(const ObjectID& object_id, int error_type) {
  const std::string meta = std::to_string(error_type);
  Status status = store_client_.CreateAndSeal(object_id, "", meta);
  if (status.IsObjectExists()) {
    // It is ok for us if the object is already marked as failed.
    return Status::OK();
  }
  return status;
}

Status ObjectStoreCallIPC::PinObjects(const std::vector<ObjectID>& object_ids) {
  std::vector<plasma::ObjectBuffer> plasma_results;
  // TODO(swang): This `Get` has a timeout of 0, so the plasma store will not
  // block when serving the request. However, if the plasma store is under
  // heavy load, then this request can still block the NodeManager event loop
  // since we must wait for the plasma store's reply. We should consider using
  // an `AsyncGet` instead.
  RAY_RETURN_NOT_OK(store_client_.Get(object_ids, /*timeout_ms=*/0, &plasma_results));

  // Pin the requested objects until the owner notifies us that the objects can be
  // unpinned by responding to the WaitForObjectEviction message.
  for (int64_t i = 0; i < object_ids.size(); i++) {
    const ObjectID& object_id = object_ids[i];

    if (plasma_results[i].data == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                      << " was evicted before the raylet could pin it.";
      continue;
    }

    RAY_LOG(DEBUG) << "Pinning object " << object_id;
    RAY_CHECK(
        pinned_objects_
            .emplace(
                object_id,
                std::unique_ptr<RayObject>(new RayObject(
                    std::make_shared<PlasmaBuffer>(plasma_results[i].data),
                    std::make_shared<PlasmaBuffer>(plasma_results[i].metadata), {})))
            .second);
  }
  return Status::OK();
}

Status ObjectStoreCallIPC::UnPinObject(const ObjectID& object_id) {
  RAY_LOG(DEBUG) << "Unpinning object " << object_id;
  pinned_objects_.erase(object_id);
  return Status::OK();
}

}  // namespace ray
