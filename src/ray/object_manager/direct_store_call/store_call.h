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

#include "ray/common/id.h"
#include "ray/common/status.h"

namespace ray {

class ObjectStoreCall {
 public:
  virtual Status MarkObjectAsFailed(const ObjectID& object_id, int error_type) = 0;
  virtual Status PinObjects(const std::vector<ObjectID>& object_ids) = 0;
  virtual Status UnPinObject(const ObjectID& object_id) = 0;
};

}  // namespace ray
