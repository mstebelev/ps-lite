#pragma once
#include <unordered_set>
#include "base/common.h"
#include "base/range.h"
#include "proto/node.pb.h"
#include "proto/data.pb.h"
namespace ps {

// assign *node* with proper rank_id, key_range, etc..
class NodeAssigner {
 public:
  NodeAssigner(int num_servers, Range<Key> key_range) {
    num_servers_ = num_servers;
    key_range_ = key_range;
  }
  ~NodeAssigner() { }

  void Assign(Node* node) {
    Range<Key> kr = key_range_;
    int rank = 0;
    if (node->role() == Node::SERVER) {
      rank = GetRankForNode(node, server_ranks_);
      kr = key_range_.EvenDivide(num_servers_, rank);

      LOG(INFO) << "node " << rank << " key range [" << kr.begin() <<", " << kr.end() << ") num servers " << num_servers_;
    } else if (node->role() == Node::WORKER) {
      rank = GetRankForNode(node, worker_ranks_);
    }
    node->set_rank(rank);
    kr.To(node->mutable_key());
  }

  void Remove(const Node& node) {
    // TODO...
  }
 protected:
  int GetRankForNode(Node * node, std::unordered_set<int> & ranks) {
    if (node->has_rank()) {
      if (!ranks.insert(node->rank()).second) {
        LOG(FATAL) << "Duplicate rank: " << node->rank();
      }
      return node->rank();
    }
    return GetNextRank(ranks);
  }

  int GetNextRank(std::unordered_set<int> & ranks) {
    for (int i = 0; i < num_servers_; ++i) {
      if (ranks.insert(i).second) {
        return i;
      }
    }
    LOG(FATAL) << "Too many nodes: " << num_servers_;
  }

  int num_servers_ = 0;
  std::unordered_set<int> server_ranks_;
  std::unordered_set<int> worker_ranks_;
  Range<Key> key_range_;
};

} // namespace ps
