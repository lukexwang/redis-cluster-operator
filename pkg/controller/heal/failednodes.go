package heal

import (
	"k8s.io/apimachinery/pkg/util/errors"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
)

// FixFailedNodes fix failed nodes: in some cases (cluster without enough master after crash or scale down), some nodes may still know about fail nodes
// 修复失败的nodes: 在某些情况下(如在master crash 或 缩容后,集群没有足够的master), 某些节点处于fail 状态且这个状态其他node(cluster nodes命令)中依然能看到
// 将上面这些nodes在所有node中 forget掉,如下面这种node就可以forget掉:
// 7cb5f2c93e38011bafd18f223217bb25cef76fab :0@0 master,fail,noaddr - 1616398534141 1616398528000 218 disconnected
func (c *CheckAndHeal) FixFailedNodes(cluster *redisv1alpha1.DistributedRedisCluster, infos *redisutil.ClusterInfos, admin redisutil.IAdmin) (bool, error) {
	forgetSet := listGhostNodes(cluster, infos)
	var errs []error
	doneAnAction := false
	for id := range forgetSet {
		doneAnAction = true
		c.Logger.Info("[FixFailedNodes] Forgetting failed node, this command might fail, this is not an error", "node", id)
		if !c.DryRun {
			c.Logger.Info("[FixFailedNodes] try to forget node", "nodeId", id)
			if err := admin.ForgetNode(id); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return doneAnAction, errors.NewAggregate(errs)
}

// listGhostNodes : A Ghost node is a node still known by some redis node but which doesn't exists anymore
// meaning it is failed, and pod not in kubernetes, or without targetable IP
// Ghost Node是指: 一个node其实已经不存在(kubernetes中已经没有该节点 或 目的IP已经不存在了)但是还在被一些redis节点记住(cluster nodes 依然能找到)
// 例如:
// 7cb5f2c93e38011bafd18f223217bb25cef76fab :0@0 master,fail,noaddr - 1616398534141 1616398528000 218 disconnected
func listGhostNodes(cluster *redisv1alpha1.DistributedRedisCluster, infos *redisutil.ClusterInfos) map[string]bool {
	ghostNodesSet := map[string]bool{}
	if infos == nil || infos.Infos == nil {
		return ghostNodesSet
	}
	for _, nodeinfos := range infos.Infos {
		for _, node := range nodeinfos.Friends {
			// only forget it when no more part of kubernetes, or if noaddress
			if node.HasStatus(redisutil.NodeStatusNoAddr) {
				ghostNodesSet[node.ID] = true
			}
			if node.HasStatus(redisutil.NodeStatusFail) || node.HasStatus(redisutil.NodeStatusPFail) {
				found := false
				for _, pod := range cluster.Status.Nodes {
					if pod.ID == node.ID {
						found = true
					}
				}
				if !found {
					ghostNodesSet[node.ID] = true
				}
			}
		}
	}
	return ghostNodesSet
}
