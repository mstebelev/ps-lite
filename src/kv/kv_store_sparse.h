#pragma once
#include "kv/kv_store.h"
#include "base/thread_pool.h"
#include "ps/node_info.h"
namespace ps {

template<typename K, typename E, typename V, typename Handle>
class KVStoreSparse : public KVStore {
 public:
  KVStoreSparse(int id, Handle handle, int pull_val_len, int nt)
      : KVStore(id), handle_(handle), k_(pull_val_len), nt_(nt), pool_(nt) {
    CHECK_GT(k_, 0); CHECK_GT(nt_, 0); CHECK_LT(nt_, 30);
    data_.resize(nt_);
    auto kr = NodeInfo::KeyRange();
    min_key_ = kr.begin();
    bucket_size_ = (kr.end() - kr.begin() -1 ) / nt_ + 1;
    key_pos_.resize(nt_+1);
    pool_.StartWorkers();
  }

  virtual ~KVStoreSparse() { }

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
        auto it = data_[0].find(key_i);
        if (it != data_[0].end()) {
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

      SliceKey(key.data(), n);

      for (int i = 0; i < nt_; ++i) {
        pool_.Add([this, &key, &val, n, i](){
            ThreadPull(key.data(), val.data(), n, k_, i); });
      }
      pool_.Wait();

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
        auto it = data_[0].find(key_i);
        if (it != data_[0].end()) {
            handle_.Push(key_i, Blob<const V>(val_data, k), it->second, false);
        } else {
            E v;
            if (handle_.Push(key_i, Blob<const V>(val_data, k), v, true)) {
                data_[0][key_i] = v;
            }
        }
        val_data += k;
      }
    } else if (!dyn && n) {
      CHECK_EQ(msg->value.size(), (size_t)1);
      SArray<V> val(msg->value[0]);
      size_t k = val.size() / n;
      CHECK_EQ(k * n, val.size());

      SliceKey(key.data(), n);

      for (int i = 0; i < nt_; ++i) {
        pool_.Add([this, &key, &val, n, k, i](){
            ThreadPush(key.data(), val.data(), n, k, i); });
      }
      pool_.Wait();
    }

    FinishReceivedRequest(ts, msg->sender);
    handle_.Finish();
  }

  virtual void Load(dmlc::Stream *fi, bool full_state_mode) override {
    handle_.Load(fi);
    K key;
    while (true) {
      if (fi->Read(&key, sizeof(K)) != sizeof(K)) break;
      GetValue(key).Load(fi, full_state_mode);
    }
    int size = 0;
    for (int i = 0; i < nt_; ++i) {
      LOG(INFO) << "bucket " << i << " [" <<
          min_key_ + i * bucket_size_ << ", " <<
          min_key_ + (i+1) * bucket_size_ << ") " <<
          data_[i].size();
      size += data_[i].size();
    }
    LOG(INFO) << "loaded " << size << " kv pairs in total";
  }

  virtual void Save(dmlc::Stream *fo, bool full_state_mode) const override {
    handle_.Save(fo);
    int saved = 0;

    for (int i = 0; i < nt_; ++i) {
      int s = 0;
      for (const auto& it : data_[i]) {
        if (!full_state_mode && it.second.Empty()) continue;
        fo->Write(&it.first, sizeof(K));
        it.second.Save(fo, full_state_mode);
        ++ s;
      }
      LOG(INFO) << "bucket " << i << " [" <<
          min_key_ + i * bucket_size_ << ", " <<
          min_key_ + (i+1) * bucket_size_ << "): " << s;
      saved += s;
    }
    LOG(INFO) << "saved " << saved << " kv pairs in total";
  }

 private:
  std::vector<std::unordered_map<K, E>> data_;
  Handle handle_;
  int k_, nt_;

  K min_key_;
  K bucket_size_;

  ThreadPool pool_;

  std::vector<int> key_pos_;

  void SliceKey(K* key, int n) {
    key_pos_[0] = 0;
    for (int i = 1; i < nt_; ++i) {
      K k = min_key_ + bucket_size_ * i;
      key_pos_[i] = std::lower_bound(key + key_pos_[i-1], key + n, k) - key;
      LOG(ERROR) << key_pos_[i] - key_pos_[i-1];
    }
    key_pos_[nt_] = n;
  }

  E& GetValue(K key) {
    return data_[(key - min_key_) / bucket_size_][key];
  }

  void ThreadPush(K* key, V* val, int n, int k, int tid) {
    auto& data = data_[tid];
    val += key_pos_[tid] * k;
    for (int i = key_pos_[tid]; i < key_pos_[tid+1]; ++i, val += k) {
      K key_i = key[i];
      auto it = data.find(key_i);
      if (it != data.end()) {
          handle_.Push(key_i, Blob<const V>(val, k), it->second, false);
      } else {
          E v;
          if (handle_.Push(key_i, Blob<const V>(val, k), v, true)) {
              data[key_i] = v;
          }
      }
    }
  }

  void ThreadPull(K* key, V* val, int n, int k, int tid) {
    auto& data = data_[tid];
    val += key_pos_[tid] * k;
    for (int i = key_pos_[tid]; i < key_pos_[tid+1]; ++i, val += k) {
      K key_i = key[i];
      Blob<V> pull(val, k);
      auto it = data.find(key_i);
      if (it != data.end()) {
          handle_.Pull(key_i, it->second, pull);
      } else {
          E v;
          handle_.Pull(key_i, v, pull);
      }
      CHECK_EQ(pull.size, (size_t)k) << "use dyanmic pull";
      if (pull.data != val) {
        memcpy(val, pull.data, sizeof(V)*k);
      }
    }
  }
};
}  // namespace ps
