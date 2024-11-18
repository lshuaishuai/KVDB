以下是 Raft-based KV 存储系统（如 RaftKVServer）从客户端发起请求到最终得到响应的详细流程，包括日志复制、选举、心跳、状态机的更新等。
这个流程假设你在一个 Raft 集群中有多个节点（包括 Leader 和 Follower），并且客户端请求会首先到达 Leader 节点。
实现启动raftKDB服务端，每个raftNode都有一个KVServer和Persister，各个节点读取磁盘中持久化的数据；每个子进程对应一个KServer，将当前的raftNode与其余节点建立连接，用来进行RPC通信，KVServer与raftNode使用ApplyChan队列进行通信，客户端与KVServer使用另一个RPC进行通信。
启动客户端，与所有的KVServer的raftNode建立连接，Clerk类对象；
# 1. 客户端发起请求
假设客户端发起了一个 PutAppend 请求（或 Get 请求），它向 Raft 集群中的一个节点发送请求。
请求内容：客户端请求包含操作类型（Put 或 Append），键，值，客户端 ID 和请求 ID。
请求目标：请求最终会到达集群的 Leader 节点（如果当前没有 Leader，Raft 会触发选举，Leader 会在选举完成后处理请求）。
# 2. Leader 节点接收请求
当 Leader 节点收到请求后(通过RPC收到请求)，它会处理这个请求并生成一个日志条目。这通常包括以下几个步骤：
生成日志条目：Leader 将请求封装为一个日志条目（例如一个 Op 对象），记录操作类型、键、值等。
追加到本地日志：将这个日志条目追加到 Leader 节点的本地日志（m_logs）中。
提交日志：Leader 节点会将这个日志条目标记为待提交，等待其他 Follower 节点的确认。
# 3. Leader 节点向 Follower 节点发送日志复制请求（AppendEntries RPC）
Leader 向所有的 Follower 节点发送 AppendEntries 请求，这个请求包含以下信息：
日志条目内容（即客户端的请求）
当前 Leader 的任期号
Leader 的最后一个日志条目的索引和任期号（用于保证日志的一致性）
# 4. Follower 节点处理日志复制请求
每个 Follower 节点收到 AppendEntries 请求后，会做以下几件事：
日志一致性检查：Follower 会检查自己的日志是否有与 Leader 发送的日志条目匹配。如果匹配（索引和任期号一致），则接受并将该日志条目复制到本地日志。
返回响应：Follower 会向 Leader 返回一个响应，告知 Leader 是否成功复制该条日志。
# 5. Leader 收集 Follower 的响应
Leader 节点会等待所有的 Follower 节点的响应。如果 大多数 节点（包括 Leader 自身）成功复制了日志条目，则说明该日志条目已经 提交。
日志提交：当 Leader 收到大多数节点确认后，它会将日志条目标记为已提交，并通知所有 Follower 节点这个日志条目已经提交。
通知状态机应用日志：Leader 会将已提交的日志条目发送到状态机（例如 KV 存储的 applych 通道），并让状态机应用这个命令（执行 Put 或 Append 操作）。
# 6. Leader 向状态机应用日志
Leader 节点将日志条目添加到 applych（用于状态机应用日志），状态机（KV 存储）会从 applych 中读取这些日志条目，并应用它们。这通常涉及到更新存储中的键值对。
例如，Leader 将请求中的 PutAppend 操作应用到 KV 存储中，修改或添加键值对。
# 7. 状态机执行结果并发送响应
等待应用完成：状态机在应用完日志条目后，会将执行结果（例如成功或失败）放入一个通道（waitApplyCh）。
通知客户端：Leader 节点会从 waitApplyCh 中读取结果，并将执行结果通过 RPC 返回给客户端。客户端接收到响应后，完成请求处理。
# 8. 客户端得到响应
客户端收到 Leader 节点的响应后，成功处理请求。如果在处理过程中遇到任何错误（如 NotLeader 错误，表示请求发送到非 Leader 节点），客户端可能会重新选择正确的 Leader 重试请求。
# 9. 选举和 Leader 变更（如果发生）
在某些情况下，Leader 可能会失去与 Follower 的连接，或者因为心跳超时导致的选举失败，集群会触发选举过程，选择一个新的 Leader。这个过程包括：
心跳丢失：如果 Leader 在一定时间内没有向 Follower 节点发送心跳，Follower 会认为 Leader 崩溃或失效，进而发起选举。
发起选举：Follower 会开始选举自己为新的候选人，并向其他节点发送 RequestVote 请求，争取选票。
选举胜利：如果一个候选人得到了大多数节点的选票，它就会成为新的 Leader，并开始处理客户端请求。
# 10. 心跳（Heartbeats）
为了防止 Follower 节点过早地发起选举，Leader 会定期发送心跳（AppendEntries 请求，其中没有日志条目）给所有的 Follower 节点。
心跳的作用：心跳消息使得 Follower 节点知道 Leader 仍然存活，防止 Follower 节点发起不必要的选举。
保持 Leader 的地位：通过心跳，Leader 节点能够维持其领导地位，避免集群因为网络延迟或暂时失联而发生 Leader 变更。
# 11. 日志一致性检查和修复（日志裁剪）
在 Leader 节点重新选举后，或者在日志发生冲突时，Raft 协议需要对日志一致性进行修复，确保所有节点的日志条目是相同的。
日志冲突检测：Leader 节点通过对比自己的日志与 Follower 节点的日志，发现日志不一致时，回退 Follower 的日志条目，重新复制正确的日志条目。
日志裁剪：Leader 会定期删除已应用且不再需要的日志条目（垃圾回收）。
# 12. Raft 节点的生命周期
Follower 节点：Follower 节点接收并复制 Leader 的日志条目，但不会主动处理客户端请求，只有当被选举为 Leader 后才会处理客户端请求。
Candidate 节点：当 Follower 节点在超时时间内没有收到 Leader 的心跳时，会发起选举，转为 Candidate，争取选票，直到当选为 Leader 或者再次变回 Follower。
