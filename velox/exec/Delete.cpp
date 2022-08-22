/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/exec/Delete.h"
#include "velox/exec/Task.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

Delete::Delete(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::DeleteNode>& deleteNode)
    : Operator(
          driverCtx,
          deleteNode->outputType(),
          operatorId,
          deleteNode->id(),
          "Delete"),
      numDeletedRows_(0),
      finished_(false),
      closed_(false),
      driverCtx_(driverCtx) {
  auto inputType = deleteNode->sources()[0]->outputType();
  auto channel = exprToChannel(deleteNode->rowId().get(), inputType);
  VELOX_CHECK_NE(
      channel,
      kConstantChannel,
      "Delete doesn't allow constant row id channel");
  rowIdChannel_ = channel;
}

void Delete::addInput(RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }

  if (!updatableDataSourceSupplier_) {
    auto updatableDataSourceSupplier
        = driverCtx_->driver->updatableDataSourceSupplier();
    VELOX_CHECK_NOT_NULL(updatableDataSourceSupplier, "UpdatableDataSource is missing");
    updatableDataSourceSupplier_ = updatableDataSourceSupplier;
  }

  const auto& updatable = updatableDataSourceSupplier_();
  if (updatable.has_value()) {
    std::cout << "Delete get updatable@" << updatable->get() << std::endl;
    const auto& ids = input->childAt(rowIdChannel_);
    updatable.value()->deleteRows(ids);
    numDeletedRows_ += ids->size();
  }
}

RowVectorPtr Delete::getOutput() {
  // Making sure the output is read only once after the deletion is fully done
  if (!noMoreInput_ || finished_) {
    return nullptr;
  }
  finished_ = true;

  auto rowsDeleted = std::dynamic_pointer_cast<FlatVector<int64_t>>(
      BaseVector::create(BIGINT(), 1, pool()));
  rowsDeleted->set(0, numDeletedRows_);

  std::vector<VectorPtr> columns = {rowsDeleted};

  // TODO Find a way to not have this Presto-specific logic in here.
  if (outputType_->size() > 1) {
    auto fragments = std::dynamic_pointer_cast<FlatVector<StringView>>(
        BaseVector::create(VARBINARY(), 1, pool()));
    fragments->setNull(0, true);
    columns.emplace_back(fragments);

    // clang-format off
    auto commitContextJson = folly::toJson(
        folly::dynamic::object
            ("lifespan", "TaskWide")
            ("taskId", driverCtx_->task->taskId())
            ("pageSinkCommitStrategy", "NO_COMMIT")
            ("lastPage", false));
    // clang-format on

    auto commitContext = std::make_shared<ConstantVector<StringView>>(
        pool(), 1, false, VARBINARY(), StringView(commitContextJson));
    columns.emplace_back(commitContext);
  }

  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), 1, columns);
}

} // namespace facebook::velox::exec
