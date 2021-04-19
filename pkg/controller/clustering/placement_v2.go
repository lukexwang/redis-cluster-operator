package clustering

import (
	"fmt"

	"github.com/go-logr/logr"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/statefulsets"
)

type Ctx struct {
	log               logr.Logger
	expectedMasterNum int
	clusterName       string
	cluster           *redisutil.Cluster
	nodes             map[string]redisutil.Nodes //key: stateFulSetName,value: 该stateFulSet的nodes信息
	currentMasters    redisutil.Nodes            //保存 所有负责slot的master的节点信息
	newMastersBySts   map[string]*redisutil.Node //key: stateFulSetName,value: 该stateFulSet对应的master node信息(负责slot的stateFulSet, 没有负责slot的stateFulSet 的master信息都在这里面)
	slavesByMaster    map[string]redisutil.Nodes //key: master.ID, values: 该stateFulSet的slave nodes(非master nodes,其他的都是slave nodes)信息
	bestEffort        bool
}

func NewCtx(cluster *redisutil.Cluster, nodes redisutil.Nodes, masterNum int32, clusterName string, log logr.Logger) *Ctx {
	ctx := &Ctx{
		log:               log,
		expectedMasterNum: int(masterNum),
		clusterName:       clusterName,
		cluster:           cluster,
		slavesByMaster:    make(map[string]redisutil.Nodes),
		newMastersBySts:   make(map[string]*redisutil.Node),
	}
	ctx.nodes = ctx.sortRedisNodeByStatefulSet(nodes) //map[string]nodes, key: stateFulSetName,value: 该stateFulSet的nodes信息
	return ctx
}

//sortRedisNodeByStatefulSet
//- 返回map[string]nodes, key: stateFulSetName,value: 该stateFulSet的nodes信息
//- 更新 c.currentMasters, 该属性中保存 所有负责slot的master的节点信息;
func (c *Ctx) sortRedisNodeByStatefulSet(nodes redisutil.Nodes) map[string]redisutil.Nodes {
	nodesByStatefulSet := make(map[string]redisutil.Nodes)

	for _, rNode := range nodes {
		cNode, err := c.cluster.GetNodeByID(rNode.ID)
		if err != nil {
			c.log.Error(err, "[sortRedisNodeByStatefulSet] unable fo found the Cluster.Node with redis", "ID", rNode.ID)
			continue // if not then next line with cNode.Pod will cause a panic since cNode is nil
		}
		ssName := unknownVMName
		if cNode.StatefulSet != "" {
			ssName = cNode.StatefulSet
		}
		if _, ok := nodesByStatefulSet[ssName]; !ok {
			nodesByStatefulSet[ssName] = redisutil.Nodes{}
		}
		nodesByStatefulSet[ssName] = append(nodesByStatefulSet[ssName], rNode)
		if (rNode.GetRole() == redisv1alpha1.RedisClusterNodeRoleMaster) && rNode.TotalSlots() > 0 {
			c.currentMasters = append(c.currentMasters, rNode)
		}
	}

	return nodesByStatefulSet
}

//DispatchMasters 调度master
// - 为每个stateFulSet确定一个redis master节点;保存在 c.newMasterBySts
// - 对于stateFulSet下没找到任何master的情况,用合适的方法找到一个redis master;
func (c *Ctx) DispatchMasters() error {
	for i := 0; i < c.expectedMasterNum; i++ {
		stsName := statefulsets.ClusterStatefulSetName(c.clusterName, i)
		nodes, ok := c.nodes[stsName]
		if !ok {
			return fmt.Errorf("missing statefulset %s", stsName)
		}
		//获取(statefulSet的)nodes负责slots的master节点
		currentMasterNodes := nodes.FilterByFunc(redisutil.IsMasterWithSlot)
		if len(currentMasterNodes) == 0 {
			//这个stateFulSet没任何master负责slots
			master := c.PlaceMasters(stsName)
			c.newMastersBySts[stsName] = master
		} else if len(currentMasterNodes) == 1 {
			//这个stateFulSet有一个master负责slots
			c.newMastersBySts[stsName] = currentMasterNodes[0]
		} else if len(currentMasterNodes) > 1 {
			//这个stateFulSet有多个master负责slots,可能发生了脑裂
			c.log.Error(fmt.Errorf("split brain"), "fix manually", "statefulSet", stsName, "masters", currentMasterNodes)
			return fmt.Errorf("split brain: %s", stsName)
		}
	}

	return nil
}

//PlaceMasters 该函数背景是stateFulSet下没找到任何 master node,所以目标就是在stateFulSet的所有redis nodes中选择一个节点来做master
// - 优先选择一个cNode,该cNode 和 现有其他master node都不在一个母机上;
// - 如果上面没有找到一个 cNode 和 所有其他master node都不在一个母机上,那么默认返回 stateFulSet的第一个 redis node作为master
// - 可以想象: 如果新增一个stateFulSet, 2个redis node,此时两个redis node都是 master; 所以选择谁做master 是策略问题;
func (c *Ctx) PlaceMasters(ssName string) *redisutil.Node {
	//把所有master Node都保存在 allMasters中
	var allMasters redisutil.Nodes
	allMasters = append(allMasters, c.currentMasters...)
	for _, master := range c.newMastersBySts {
		allMasters = append(allMasters, master)
	}
	nodes := c.nodes[ssName] //获得stateFulSet下所有redis node
	for _, cNode := range nodes {
		_, err := allMasters.GetNodesByFunc(func(node *redisutil.Node) bool {
			if node.NodeName == cNode.NodeName {
				return true
			}
			return false
		})
		//对于GetNodesByFunc来说,只会发生 notFound错误;
		//所以出现notFound错误时,代表cNode 和 所有其他master node都不在一个母机上,则选择cNode作为stateFulSet的master
		if err != nil {
			return cNode
		}
	}
	c.bestEffort = true
	c.log.Info("the pod are not spread enough on VMs to have only one master by VM", "select", nodes[0].IP)
	//如果上面没有找到一个 cNode 和 所有其他master node都不在一个母机上,那么默认返回 stateFulSet的第一个 redis node作为master
	return nodes[0]
}

//PlaceSlaves c.slavesByMaster: key: master.ID, values: 该stateFulSet的slave nodes(非master nodes,其他的都是slave nodes)信息
func (c *Ctx) PlaceSlaves() error {
	c.bestEffort = true
	for ssName, nodes := range c.nodes {
		master := c.newMastersBySts[ssName]
		for _, node := range nodes {
			if node.IP == master.IP {
				continue
			}
			if node.NodeName != master.NodeName {
				c.bestEffort = false
			}
			if node.GetRole() == redisv1alpha1.RedisClusterNodeRoleSlave {
				if node.MasterReferent != master.ID { //如果该stateFulSet中slave的master ID 不是 该stateFulSet的master,报错
					c.log.Error(nil, "master referent conflict", "node ip", node.IP,
						"current masterID", node.MasterReferent, "expect masterID", master.ID, "master IP", master.IP)
					c.slavesByMaster[master.ID] = append(c.slavesByMaster[master.ID], node)
				}
				continue
			}
			c.slavesByMaster[master.ID] = append(c.slavesByMaster[master.ID], node)
		}
	}
	return nil
}

func (c *Ctx) GetCurrentMasters() redisutil.Nodes {
	return c.currentMasters
}

func (c *Ctx) GetNewMasters() redisutil.Nodes {
	var nodes redisutil.Nodes
	for _, node := range c.newMastersBySts {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Ctx) GetSlaves() map[string]redisutil.Nodes {
	return c.slavesByMaster
}

func (c *Ctx) GetStatefulsetNodes() map[string]redisutil.Nodes {
	return c.nodes
}
