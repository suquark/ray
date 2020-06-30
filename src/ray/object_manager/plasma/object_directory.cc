// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/object_manager/plasma/object_directory.h"
#include "ray/object_manager/plasma/malloc.h"
#include "ray/object_manager/plasma/plasma_allocator.h"

namespace plasma {

void ObjectTableEntry::FreeObject() {
  int64_t buff_size = ObjectSize();
  if (device_num == 0) {
    PlasmaAllocator::Free(pointer, buff_size);
  } else {
#ifdef PLASMA_CUDA
    ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_num - 1));
    RAY_CHECK_OK(context->Free(pointer, buff_size));
#endif
  }
  Reset();
  state = ObjectState::PLASMA_EVICTED;
}

Status ObjectTableEntry::AllocateMemory(int device_id, size_t size) {
  if (device_id == 0) {
    // Allocate space for the new object. We use memalign instead of malloc
    // in order to align the allocated region to a 64-byte boundary. This is not
    // strictly necessary, but it is an optimization that could speed up the
    // computation of a hash of the data (see compute_object_hash_parallel in
    // plasma_client.cc). Note that even though this pointer is 64-byte aligned,
    // it is not guaranteed that the corresponding pointer in the client will be
    // 64-byte aligned, but in practice it often will be.
    uint8_t* address = reinterpret_cast<uint8_t*>(PlasmaAllocator::Memalign(kBlockSize, size));
    if (!address) {
      Reset();
      return Status::ObjectStoreFull("Cannot allocate object.");
    }
    pointer = address;
    GetMallocMapinfo(pointer, &fd, &map_size, &offset);
    RAY_CHECK(fd != -1);
  } else {
#ifdef PLASMA_CUDA
    RAY_DCHECK(device_id != 0);
    ARROW_ASSIGN_OR_RAISE(auto context, manager_->GetContext(device_id - 1));
    ARROW_ASSIGN_OR_RAISE(auto cuda_buffer, context->Allocate(static_cast<int64_t>(size)));
    // The IPC handle will keep the buffer memory alive
    Status s = cuda_buffer->ExportForIpc().Value(&ipc_handle);
    if (!s.ok()) {
      Reset();
      RAY_LOG(ERROR) << "Failed to allocate CUDA memory: " << s.ToString();
      return s;
    }
    pointer = reinterpret_cast<uint8_t*>(cuda_buffer->address());
    // GPU objects do not have map_size.
    map_size = 0;
#else
  RAY_LOG(ERROR) << "device_num != 0 but CUDA not enabled";
  return Status::OutOfMemory("CUDA is not enabled.");
#endif
  }
  state = ObjectState::PLASMA_CREATED;
  device_num = device_id;
  create_time = std::time(nullptr);
  construct_duration = -1;
  return Status::OK();
}

void PlasmaObject_init(PlasmaObject* object, ObjectTableEntry* entry) {
  RAY_DCHECK(object != nullptr);
  RAY_DCHECK(entry != nullptr);
  // RAY_DCHECK(entry->state == ObjectState::PLASMA_SEALED);
#ifdef PLASMA_CUDA
  if (entry->device_num != 0) {
    object->ipc_handle = entry->ipc_handle;
  }
#endif
  object->store_fd = entry->fd;
  RAY_LOG(INFO) << "STORE_FD:STORE_FD:STORE_FD:STORE_FD:STORE_FD:STORE_FD:" << object->store_fd;
  object->data_offset = entry->offset;
  object->metadata_offset = entry->offset + entry->data_size;
  object->data_size = entry->data_size;
  object->metadata_size = entry->metadata_size;
  object->device_num = entry->device_num;
  object->map_size = entry->map_size;
  RAY_LOG(INFO) << "MAP_SIZE:MAP_SIZE:MAP_SIZE:MAP_SIZE:MAP_SIZE:MAP_SIZE:" << object->map_size;
  object->initialized = true;
}

ObjectDirectory::ObjectDirectory(
  const std::shared_ptr<ExternalStore> &external_store,
  const std::function<void(const std::vector<ObjectInfoT>&)> &notifications_callback) :
    eviction_policy_(this, PlasmaAllocator::GetFootprintLimit()),
    external_store_(external_store),
    notifications_callback_(notifications_callback) {}

void ObjectDirectory::GetSealedObjectsInfo(std::vector<ObjectInfoT>* infos) {
  absl::MutexLock lock(&object_table_mutex_);
  for (const auto& entry : object_table_) {
    if (entry.second->state == ObjectState::PLASMA_SEALED) {
      ObjectInfoT info;
      info.object_id = entry.first.Binary();
      info.data_size = entry.second->data_size;
      info.metadata_size = entry.second->metadata_size;
      infos->push_back(info);
    }
  }
}

ObjectStatus ObjectDirectory::ContainsObject(const ObjectID& object_id) {
  absl::MutexLock lock(&object_table_mutex_);
  auto entry = GetObjectTableEntry(object_id);
  return entry && (entry->state == ObjectState::PLASMA_SEALED ||
                  entry->state == ObjectState::PLASMA_EVICTED)
            ? ObjectStatus::OBJECT_FOUND
            : ObjectStatus::OBJECT_NOT_FOUND;
}

void ObjectDirectory::GetObjects(
    const std::vector<ObjectID>& object_ids,
    Client* client,
    std::vector<ObjectID> *sealed_objects,
    std::vector<ObjectID> *reconstructed_objects,
    std::vector<ObjectID> *nonexistent_objects) {
  absl::MutexLock lock(&object_table_mutex_);
  std::vector<ObjectID> evicted_ids;
  std::vector<ObjectTableEntry*> entries;

  for (auto object_id : object_ids) {
    auto it = object_table_.find(object_id);
    if (it == object_table_.end()) {
      nonexistent_objects->push_back(object_id);
    } else {
      auto& entry = it->second;
      switch (entry->state) {
        case ObjectState::PLASMA_SEALED: {
          sealed_objects->push_back(object_id);
        } break;
        case ObjectState::PLASMA_EVICTED: {
          Status status = AllocateMemory(object_id, entry.get(), entry->ObjectSize(),
                                          /*evict_inf_full=*/true, client, /*is_create=*/false,
                                          entry->device_num);
          if (status.ok()) {
            evicted_ids.push_back(object_id);
          } else {
            // We are out of memory and cannot allocate memory for this object.
            // Change the state of the object back to PLASMA_EVICTED so some
            // other request can try again.
            entry->state = ObjectState::PLASMA_EVICTED;
          }
        } break;
        default:
          nonexistent_objects->push_back(object_id);
      }
    }
  }

  if (!evicted_ids.empty()) {
    if (external_store_) {
      std::vector<std::shared_ptr<Buffer>> buffers;
      for (const auto& entry : entries) {
        buffers.emplace_back(entry->GetArrowBuffer());
      }
      Status status = external_store_->Get(object_ids, buffers);
      if (status.ok()) {
        // We have successfully reconstructed these objects. Mark these
        // objects are sealed.
        for (const auto& entry : entries) {
          entry->state = ObjectState::PLASMA_SEALED;
          entry->construct_duration =  std::time(nullptr) - entry->create_time;
        }
        // Flush to reconstructed objects.
        RAY_CHECK(reconstructed_objects->empty());
        reconstructed_objects->swap(evicted_ids);
        return;
      }
    }
    // We tried to get the objects from the external store, but could not get them.
    // Set the state of these objects back to PLASMA_EVICTED so some other request
    // can try again.
    for (const auto& entry : entries) {
      entry->state = ObjectState::PLASMA_EVICTED;
    }
  }
}

Status ObjectDirectory::CreateObject(const ObjectID& object_id, bool evict_if_full,
                                     int64_t data_size, int64_t metadata_size,
                                     int device_num, Client* client,
                                     PlasmaObject* result) {
  absl::MutexLock lock(&object_table_mutex_);
  ObjectTableEntry* entry;
  RAY_RETURN_NOT_OK(CreateObjectInternal(
    object_id, evict_if_full, data_size, metadata_size, device_num, client, &entry));
  PlasmaObject_init(result, entry);
  return Status::OK();
}

Status ObjectDirectory::CreateAndSealObject(const ObjectID& object_id, bool evict_if_full,
                                            const std::string &data, const std::string &metadata,
                                            int device_num, Client* client, PlasmaObject* result) {
  absl::MutexLock lock(&object_table_mutex_);
  RAY_CHECK(device_num == 0) << "CreateAndSeal currently only supports device_num = 0, "
                                "which corresponds to the host.";
  ObjectTableEntry* entry;
  RAY_RETURN_NOT_OK(CreateObjectInternal(
    object_id, evict_if_full, data.size(), metadata.size(), device_num, client, &entry));
  PlasmaObject_init(result, entry);
  // Write the inlined data and metadata into the allocated object.
  std::memcpy(entry->pointer, data.data(), data.size());
  std::memcpy(entry->pointer + data.size(), metadata.data(), metadata.size());
  SealObjectsInternal({object_id});
  // Remove the client from the object's array of clients because the
  // object is not being used by any client. The client was added to the
  // object's array of clients in CreateObject. This is analogous to the
  // Release call that happens in the client's Seal method.
  RAY_CHECK(RemoveFromClientObjectIds(object_id, entry, client) == 1);
  return Status::OK();
}

void ObjectDirectory::EvictObjects(int64_t num_bytes, int64_t *num_bytes_evicted) {
  absl::MutexLock lock(&object_table_mutex_);
  std::vector<ObjectID> objects_to_evict;
  *num_bytes_evicted =
      eviction_policy_.ChooseObjectsToEvict(num_bytes, &objects_to_evict);
  EvictObjectsInternal(objects_to_evict);
}

PlasmaError ObjectDirectory::DeleteObject(const ObjectID& object_id) {
  absl::MutexLock lock(&object_table_mutex_);
  auto it = object_table_.find(object_id);
  // TODO(rkn): This should probably not fail, but should instead throw an
  // error. Maybe we should also support deleting objects that have been
  // created but not sealed.
  if (it == object_table_.end()) {
    // To delete an object it must be in the object table.
    return PlasmaError::ObjectNonexistent;
  }
  auto& entry = it->second;
  if (entry->state != ObjectState::PLASMA_SEALED) {
    // To delete an object it must have been sealed.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectNotSealed;
  }

  if (entry->ref_count != 0) {
    // To delete an object, there must be no clients currently using it.
    // Put it into deletion cache, it will be deleted later.
    deletion_cache_.emplace(object_id);
    return PlasmaError::ObjectInUse;
  }
  eviction_policy_.RemoveObject(object_id);
  EraseObject(object_id);
  // Inform all subscribers that the object has been deleted.
  ObjectInfoT notification;
  notification.object_id = object_id.Binary();
  notification.is_deletion = true;
  notifications_callback_({notification});
  return PlasmaError::OK;
}

int ObjectDirectory::AbortObject(const ObjectID& object_id, Client* client) {
  absl::MutexLock lock(&object_table_mutex_);
  auto it = object_table_.find(object_id);
  // TODO(rkn): This should probably not fail, but should instead throw an
  // error. Maybe we should also support deleting objects that have been
  // created but not sealed.
  RAY_CHECK(it != object_table_.end()) << "To abort an object it must be in the object table.";
  auto& entry = it->second;
  RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
      << "To abort an object it must have been sealed.";
  auto cit = client->object_ids.find(object_id);
  if (cit == client->object_ids.end()) {
    // If the client requesting the abort is not the creator, do not
    // perform the abort.
    return 0;
  } else {
    // The client requesting the abort is the creator. Free the object.
    EraseObject(object_id);
    client->object_ids.erase(cit);
    return 1;
  }
}

void ObjectDirectory::DisconnectClient(Client* client) {
  absl::MutexLock lock(&object_table_mutex_);
  eviction_policy_.ClientDisconnected(client);
  absl::flat_hash_map<ObjectID, ObjectTableEntry*> sealed_objects;
  for (const auto& object_id : client->object_ids) {
    auto it = object_table_.find(object_id);
    if (it == object_table_.end()) {
      continue;
    }

    if (it->second->state == ObjectState::PLASMA_SEALED) {
      // Add sealed objects to a temporary list of object IDs. Do not perform
      // the remove here, since it potentially modifies the object_ids table.
      sealed_objects[it->first] = it->second.get();
    } else {
      // Abort unsealed object.
      // Don't call AbortObject() because client->object_ids would be modified.
      EraseObject(object_id);
    }
  }

  for (const auto& entry : sealed_objects) {
    RemoveFromClientObjectIds(entry.first, entry.second, client);
  }
}

void ObjectDirectory::MarkObjectAsReconstructed(const ObjectID& object_id, PlasmaObject* object) {
  absl::MutexLock lock(&object_table_mutex_);
  auto it = object_table_.find(object_id);
  RAY_CHECK(it != object_table_.end());
  auto &entry = it->second;
  PlasmaObject_init(object, entry.get());
}

void ObjectDirectory::RegisterSealedObjectToClient(const ObjectID& object_id, Client* client, PlasmaObject* object) {
  absl::MutexLock lock(&object_table_mutex_);
  auto it = object_table_.find(object_id);
  RAY_CHECK(it != object_table_.end());
  auto &entry = it->second;
  PlasmaObject_init(object, entry.get());
  // Record that this client is using this object.
  AddToClientObjectIds(object_id, entry.get(), client);
}

void ObjectDirectory::EvictObjectsInternal(const std::vector<ObjectID>& object_ids) {
  if (object_ids.empty()) {
    return;
  }

  std::vector<ObjectTableEntry*> evicted_objects_entries;
  std::vector<std::shared_ptr<arrow::Buffer>> evicted_object_data;
  std::vector<ObjectInfoT> infos;
  for (const auto& object_id : object_ids) {
    RAY_LOG(DEBUG) << "evicting object " << object_id.Hex();
    auto it = object_table_.find(object_id);
    // TODO(rkn): This should probably not fail, but should instead throw an
    // error. Maybe we should also support deleting objects that have been
    // created but not sealed.
    RAY_CHECK(it != object_table_.end()) << "To evict an object it must be in the object table.";
    auto& entry = it->second;
    RAY_CHECK(entry->state == ObjectState::PLASMA_SEALED)
        << "To evict an object it must have been sealed.";
    RAY_CHECK(entry->ref_count == 0)
        << "To evict an object, there must be no clients currently using it.";
    // If there is a backing external store, then mark object for eviction to
    // external store, free the object data pointer and keep a placeholder
    // entry in ObjectTable
    if (external_store_) {
      evicted_objects_entries.push_back(entry.get());
      evicted_object_data.emplace_back(entry->GetArrowBuffer());
    } else {
      // If there is no backing external store, just erase the object entry
      // and send a deletion notification.
      entry->FreeObject();
      // Inform all subscribers that the object has been deleted.
      ObjectInfoT notification;
      notification.object_id = object_id.Binary();
      notification.is_deletion = true;
      infos.emplace_back(notification);
    }
  }

  if (external_store_) {
    RAY_CHECK_OK(external_store_->Put(object_ids, evicted_object_data));
    for (auto entry : evicted_objects_entries) {
      entry->FreeObject();
    }
  } else {
    notifications_callback_(infos);
  }
}

Status ObjectDirectory::CreateObjectInternal(const ObjectID& object_id, bool evict_if_full,
                                             int64_t data_size, int64_t metadata_size,
                                             int device_num, Client* client,
                                             ObjectTableEntry** new_entry) {
  RAY_LOG(DEBUG) << "creating object " << object_id.Hex();
  if (object_table_.count(object_id) > 0) {
    // There is already an object with the same ID in the Plasma Store, so
    // ignore this request.
    return Status::ObjectExists("The object already exists.");
  }

  int64_t total_size = data_size + metadata_size;
  RAY_CHECK(total_size > 0) << "Memory allocation size must be a positive number.";
  auto entry = std::unique_ptr<ObjectTableEntry>(new ObjectTableEntry());
  Status s = AllocateMemory(object_id, entry.get(), total_size, evict_if_full, client, /*is_create=*/true,
                            device_num);
  if (!s.ok()) {
    return Status::OutOfMemory("Cannot allocate the object.");
  }
  entry->data_size = data_size;
  entry->metadata_size = metadata_size;
  *new_entry = entry.get();
  object_table_.emplace(object_id, std::move(entry));
  return Status::OK();
}

void ObjectDirectory::SealObjectsInternal(const std::vector<ObjectID>& object_ids) {
  std::vector<ObjectInfoT> infos;
  infos.reserve(object_ids.size());
  RAY_LOG(DEBUG) << "sealing " << object_ids.size() << " objects";
  for (size_t i = 0; i < object_ids.size(); ++i) {
    ObjectInfoT object_info;
    auto entry = GetObjectTableEntry(object_ids[i]);
    RAY_CHECK(entry != nullptr);
    RAY_CHECK(entry->state == ObjectState::PLASMA_CREATED);
    // Set the state of object to SEALED.
    entry->state = ObjectState::PLASMA_SEALED;
    // Set object construction duration.
    entry->construct_duration = std::time(nullptr) - entry->create_time;

    object_info.object_id = object_ids[i].Binary();
    object_info.data_size = entry->data_size;
    object_info.metadata_size = entry->metadata_size;
    infos.push_back(object_info);
  }
  notifications_callback_(infos);
}

void ObjectDirectory::AddToClientObjectIds(
    const ObjectID& object_id, ObjectTableEntry* entry, Client* client) {
  // Check if this client is already using the object.
  if (client->object_ids.find(object_id) != client->object_ids.end()) {
    return;
  }
  // If there are no other clients using this object, notify the eviction policy
  // that the object is being used.
  if (entry->ref_count == 0) {
    // Tell the eviction policy that this object is being used.
    eviction_policy_.BeginObjectAccess(object_id, entry->ObjectSize());
  }
  // Increase reference count.
  entry->ref_count++;

  // Add object id to the list of object ids that this client is using.
  client->object_ids.insert(object_id);
}

int ObjectDirectory::RemoveFromClientObjectIds(
    const ObjectID& object_id, ObjectTableEntry* entry, Client* client) {
  auto it = client->object_ids.find(object_id);
  if (it != client->object_ids.end()) {
    client->object_ids.erase(it);
    // Decrease reference count.
    entry->ref_count--;

    // If no more clients are using this object, notify the eviction policy
    // that the object is no longer being used.
    if (entry->ref_count == 0) {
      if (deletion_cache_.count(object_id) == 0) {
        // Tell the eviction policy that this object is no longer being used.
        eviction_policy_.EndObjectAccess(object_id, entry->ObjectSize());
      } else {
        // Above code does not really delete an object. Instead, it just put an
        // object to LRU cache which will be cleaned when the memory is not enough.
        deletion_cache_.erase(object_id);
        EvictObjectsInternal({object_id});
      }
    }
    // Return 1 to indicate that the client was removed.
    return 1;
  } else {
    // Return 0 to indicate that the client was not removed.
    return 0;
  }
}

/// Allocate memory
Status ObjectDirectory::AllocateMemory(
    const ObjectID& object_id, ObjectTableEntry* entry, size_t size, bool evict_if_full,
    Client* client, bool is_create, int device_num) {
  RAY_LOG(DEBUG) << "Allocating memory for object " << object_id.Hex()
                 << ", size = " << size << ", device = " << device_num;
  // Make sure the object pointer is not already allocated
  RAY_CHECK(!entry->pointer);
  if (device_num != 0) {
    return entry->AllocateMemory(device_num, size);
  }

  // First free up space from the client's LRU queue if quota enforcement is on.
  if (evict_if_full) {
    std::vector<ObjectID> client_objects_to_evict;
    bool quota_ok = eviction_policy_.EnforcePerClientQuota(client, size, is_create,
                                                          &client_objects_to_evict);
    if (!quota_ok) {
      return Status::OutOfMemory("Cannot assign enough quota to the client.");
    }
    EvictObjectsInternal(client_objects_to_evict);
  }

  // Try to evict objects until there is enough space.
  while (true) {
    Status s = entry->AllocateMemory(device_num, size);
    if (s.ok()) {
      // Notify the eviction policy that this object was created. This must be done
      // immediately before the call to AddToClientObjectIds so that the
      // eviction policy does not have an opportunity to evict the object.
      eviction_policy_.ObjectCreated(object_id, size, client, is_create);
      // Record that this client is using this object.
      AddToClientObjectIds(object_id, entry, client);
      return s;
    } else if (!evict_if_full) {
      return s;
    }
    // Tell the eviction policy how much space we need to create this object.
    std::vector<ObjectID> objects_to_evict;
    bool success = eviction_policy_.RequireSpace(size, &objects_to_evict);
    EvictObjectsInternal(objects_to_evict);
    // Return an error to the client if not enough space could be freed to
    // create the object.
    if (!success) {
      return Status::OutOfMemory("Fail to require enough space for the client.");
    }
  }
}

std::unique_ptr<ObjectDirectory> object_directory;
}  // namespace plasma
