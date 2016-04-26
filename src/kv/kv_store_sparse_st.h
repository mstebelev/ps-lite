#pragma once
#include "kv/kv_store.h"
namespace ps {

template<typename K, typename E, typename V, typename Handle>
class KVStoreSparseST : public KVStore {
 public:
  KVStoreSparseST(int id, Handle handle, int pull_val_len, size_t max_table_size=0)
      : KVStore(id), handle_(handle), k_(pull_val_len), max_table_size_(max_table_size) {
    CHECK_GT(k_, 0);
  }

  virtual ~KVStoreSparseST() { }

  void Clear() override {
    data_.clear();
  }

  // process a pull message
  void HandlePull(Message* msg) {
    int ts = msg->task.time();
    handle_.Start(false, ts, msg->task.cmd(), (void*)msg);
    SArray<K> key(msg->key);
    size_t n = key.size();
    SArray<V> val(n * k_);
    bool dyn = msg->task.param().dyn_val_size();
    if (dyn) {
      SArray<int> val_size(n);
      size_t start = 0;
      for (size_t i = 0; i < n; ++i) {
        K key_i = key[i];
        size_t len = val.size() - start;
        while (len < (size_t)k_) {
          val.resize(val.size()*2 + 5); len = val.size() - start;
        }
        V* val_data = val.data() + start;
        Blob<V> pull(val_data, len);
        auto it = data_.find(key_i);
        if (it != data_.end()) {
            handle_.Pull(key_i, it->second, pull);
        } else {
            E v;
            handle_.Pull(key_i, v, pull);
        }
        if (pull.data != val_data) {
          while ((start + pull.size) > val.size()) val.resize(val.size()*2 + 5);
          memcpy(val.data()+start, pull.data, sizeof(V)*pull.size);
        } else {
          CHECK_LE(pull.size, len);
        }
        start += pull.size;
        val_size[i] = pull.size;
      }
      val.resize(start);
      msg->add_value(val);
      msg->add_value(val_size);
    } else {
      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i, val_data += k_) {
        K key_i = key[i];
        Blob<V> pull(val_data, k_);
        auto it = data_.find(key_i);
        if (it != data_.end()) {
            handle_.Pull(key_i, it->second, pull);
        } else {
            E v;
            handle_.Pull(key_i, v, pull);
        }
        CHECK_EQ(pull.size, (size_t)k_) << "use dyanmic pull";
        if (pull.data != val_data) {
          memcpy(val_data, pull.data, sizeof(V)*k_);
        }
      }
      msg->add_value(val);
    }

    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  // process a push message
  void HandlePush(const Message* msg) {
    int ts = msg->task.time();
    handle_.Start(true, ts, msg->task.cmd(), (void*)msg);

    SArray<K> key(msg->key);
    size_t n = key.size();
    bool dyn = msg->task.param().dyn_val_size();

    if (dyn && n) {
      CHECK_EQ(msg->value.size(), (size_t)2);
      SArray<V> val(msg->value[0]);
      SArray<int> val_size(msg->value[1]);
      CHECK_EQ(val_size.size(), n);
      size_t len = 0;
      for (int i : val_size) len += i;
      CHECK_EQ(len, val.size());

      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i) {
        K key_i = key[i];
        size_t k = val_size[i];
        if (k == 0) continue;
        auto it = data_.find(key_i);
        if (it != data_.end()) {
            handle_.Push(key_i, Blob<const V>(val_data, k), it->second, false);
        } else {
            E v;
            if (handle_.Push(key_i, Blob<const V>(val_data, k), v, true)) {
                data_[key_i] = v;
            }
        }
        val_data += k;
      }
    } else if (!dyn && n) {
      CHECK_EQ(msg->value.size(), (size_t)1);
      SArray<V> val(msg->value[0]);
      size_t k = val.size() / n;
      CHECK_EQ(k * n, val.size());

      V* val_data = val.data();
      for (size_t i = 0; i < n; ++i, val_data += k) {
        K key_i = key[i];
        auto it = data_.find(key_i);
        if (it != data_.end()) {
            handle_.Push(key_i, Blob<const V>(val_data, k), it->second, false);
        } else {
            E v;
            if (handle_.Push(key_i, Blob<const V>(val_data, k), v, true)) {
                data_[key_i] = v;
            }
        }
      }
    }
    CleanupModel();
    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }



  void CleanupModel() {
      if (!max_table_size_ || data_.size() <= max_table_size_)
          return;
      typedef typename std::unordered_map<K, E>::const_iterator table_iterator_t;
      std::vector<table_iterator_t> data_items;
      data_items.reserve(data_.size());

      for (auto it = data_.begin(); it != data_.end(); ++it) {
          data_items.push_back(it);
      }


      auto middle = data_items.begin() + data_items.size() / 2;
      std::nth_element(data_items.begin(), middle, data_items.end(), [](const table_iterator_t & a, const table_iterator_t & b) {return a->second < b->second;});
      LOG(INFO) << "deleting " << middle - data_items.begin() << "elements from map";
      for (auto it = data_items.begin(); it < middle; ++it) {
          data_.erase(*it);
      }
  }

  virtual void Load(dmlc::Stream *fi, bool full_state_mode) override {
    handle_.Load(fi);
    K key;
    while (true) {
      if (fi->Read(&key, sizeof(K)) != sizeof(K)) break;
      data_[key].Load(fi, full_state_mode);
    }
    LOG(INFO) << "loaded " << data_.size() << " kv pairs";
  }

  virtual void Save(dmlc::Stream *fo, bool full_state_mode) const override {
    handle_.Save(fo);
    int saved = 0;
    for (const auto& it : data_) {
      if (!full_state_mode && it.second.Empty()) continue;
      fo->Write(&it.first, sizeof(K));
      it.second.Save(fo, full_state_mode);
    }
    LOG(INFO) << "saved " << saved << " kv pairs";
  }

 private:
  std::unordered_map<K, E> data_;
  Handle handle_;
  int k_;
  size_t max_table_size_;
};
}  // namespace ps
