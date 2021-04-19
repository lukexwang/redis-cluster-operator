package manager

import (
	"time"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/controller/heal"
	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
)

type IHeal interface {
	Heal(cluster *redisv1alpha1.DistributedRedisCluster, infos *redisutil.ClusterInfos, admin redisutil.IAdmin) (bool, error)
	FixTerminatingPods(cluster *redisv1alpha1.DistributedRedisCluster, maxDuration time.Duration) (bool, error)
}

type realHeal struct {
	*heal.CheckAndHeal
}

func NewHealer(heal *heal.CheckAndHeal) IHeal {
	return &realHeal{heal}
}

// Heal: 修复:0@0 master,fail,noaddr 这种节点, 修复untrusted的redis node
func (h *realHeal) Heal(cluster *redisv1alpha1.DistributedRedisCluster, infos *redisutil.ClusterInfos, admin redisutil.IAdmin) (bool, error) {
	// 修复失败的nodes: 在某些情况下(如在master crash 或 缩容后,集群没有足够的master), 某些节点处于fail 状态且这个状态其他node(cluster nodes命令)中依然能看到
	// 将上面这些nodes在所有node中 forget掉,如下面这种node就可以forget掉:
	// 7cb5f2c93e38011bafd18f223217bb25cef76fab :0@0 master,fail,noaddr - 1616398534141 1616398528000 218 disconnected
	if actionDone, err := h.FixFailedNodes(cluster, infos, admin); err != nil {
		return actionDone, err
	} else if actionDone {
		return actionDone, nil
	}

	// FixUntrustedNodes 用于删除那些 untrusted 的nodes. 一个node被移除cluster(如forget nodes命令),但又尝试join cluster
	// - 处于handshake状态的node,就是untrusted的
	// - 检查podname是否被重用,
	// 		- 如果podName没被重用,则直接删除该Pod,然后继续forget Pod;
	// 		- 如果podName被重用了,则直接删除该Pod;
	if actionDone, err := h.FixUntrustedNodes(cluster, infos, admin); err != nil {
		return actionDone, err
	} else if actionDone {
		return actionDone, nil
	}
	return false, nil
}
