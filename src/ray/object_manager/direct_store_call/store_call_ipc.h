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

#pragma once

#include <memory>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "arrow/buffer.h"

#include "ray/common/ray_object.h"
#include "ray/object_manager/direct_store_call/store_call.h"
#include "ray/object_manager/plasma/client.h"

namespace ray {

using ray::RayObject;

class ObjectStoreCallIPC: public ObjectStoreCall {
 public:
  ObjectStoreCallIPC(std::string store_socket_name);
  Status MarkObjectAsFailed(const ObjectID& object_id, int error_type);
  Status PinObjects(const std::vector<ObjectID>& object_ids);
  Status UnPinObject(const ObjectID& object_id);

 private:
  /// A Plasma object store client. This is used for creating new objects in
  /// the object store (e.g., for actor tasks that can't be run because the
  /// actor died) and to pin objects that are in scope in the cluster.
  plasma::PlasmaClient store_client_;
  absl::flat_hash_map<ObjectID, std::unique_ptr<RayObject>> pinned_objects_;
};

}  // namespace ray
