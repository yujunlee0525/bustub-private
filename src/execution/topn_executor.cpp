#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  iterator_ = 0;
  child_executor_->Init();
  auto schema = child_executor_->GetOutputSchema();
  std::priority_queue<Tuple, std::vector<Tuple>, CompareTuple> pqueue(CompareTuple(plan_, &schema));
  Tuple tuple;
  RID rid;
  tuples_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    iterator_++;
    pqueue.push(tuple);
    if (pqueue.size() > plan_->n_) {
      pqueue.pop();
      iterator_--;
    }
  }
  while (!pqueue.empty()) {
    tuples_.push_back(pqueue.top());
    pqueue.pop();
  }
  iterator_--;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ < 0) {
    return false;
  }
  *tuple = tuples_[iterator_];
  iterator_--;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return iterator_; };

}  // namespace bustub
