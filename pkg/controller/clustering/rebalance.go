package clustering

import (
	"fmt"
	"math"

	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
	"github.com/ucloud/redis-cluster-operator/pkg/utils"
)

// RebalancedCluster rebalanced a redis cluster.
// 迁移流程简介:
/*
比如当前有4个master负责 16384 个slot:
master00 0-4095
master01 4096-8191
master02 8192-12287
master03 12288-16383

4 扩容 6

expect=2730

(totalBalance>0,会做一些平衡)
master00.balance=1366
master01.balance=1366
master02.balance=1366
master03.balance=1366
master04.balance=-2732
master05.balance=-2732

排序后:
master04.balance=-2732
master05.balance=-2732
master00.balance=1366
master01.balance=1366
master02.balance=1366
master03.balance=1366

迁移流程是从源节点slot列表前面开始迁移;

master03 => master04, slot: 12288-13653, 个数: 1366, master03.balance=0,    master04.balance=-1366
master02 => master04, slot: 8192-9557,   个数: 1366, master02.balance=0,    master04.balance=0
master01 => master05, slot: 4096-5461,   个数: 1366, master01.balance=0,    master05.balance=-1366
master00 => master05, slot: 0-1365,      个数: 1366, master00.balance=0,    master05.balance=0


最后结果:
master00 slot:1366-4095,个数: 2730
master01 slot:5462-8191,个数: 2730
master02 slot:9558-12287,个数: 2729
master03 slot:13654-16383,个数: 2730
master04 slot: 8192-9557,12288-13653, 个数: 2732
master05 slot: 0-1365,4096-5461, 个数: 2732
*/
func (c *Ctx) RebalancedCluster(admin redisutil.IAdmin, newMasterNodes redisutil.Nodes) error {
	nbNode := len(newMasterNodes)
	var expected int
	for _, node := range newMasterNodes {
		expected = int(float64(admin.GetHashMaxSlot()+1) / float64(nbNode)) //每个node 期望的slot个数
		node.SetBalance(len(node.Slots) - expected)                         // node.balance 该node应该迁移出多少slot
	}

	totalBalance := 0
	for _, node := range newMasterNodes {
		totalBalance += node.Balance() //总共需要迁移的slot数
	}

	for totalBalance > 0 {
		for _, node := range newMasterNodes {
			if node.Balance() < 0 && totalBalance > 0 {
				b := node.Balance() - 1
				node.SetBalance(b)
				totalBalance -= 1
			}
		}
	}

	// Sort nodes by their slots balance.
	sn := newMasterNodes.SortByFunc(func(a, b *redisutil.Node) bool { return a.Balance() < b.Balance() })
	if log.V(4).Enabled() {
		for _, node := range sn {
			log.Info("debug rebalanced master", "node", node.IPPort(), "balance", node.Balance())
		}
	}

	log.Info(">>> rebalancing", "nodeNum", nbNode, "expected", expected)

	for _, tmpNode := range sn {
		log.Info(fmt.Sprintf("node=>%s balance:%d", tmpNode.PodName, tmpNode.Balance()))
	}

	dstIdx := 0
	srcIdx := len(sn) - 1

	for dstIdx < srcIdx {
		dst := sn[dstIdx]
		src := sn[srcIdx]

		var numSlots float64
		if math.Abs(float64(dst.Balance())) < math.Abs(float64(src.Balance())) {
			numSlots = math.Abs(float64(dst.Balance()))
		} else {
			numSlots = math.Abs(float64(src.Balance()))
		}

		if numSlots > 0 {
			log.Info(fmt.Sprintf("Moving %f slots from %s to %s", numSlots, src.PodName, dst.PodName))
			srcs := redisutil.Nodes{src} //理论上是个列表,其实里面就一个节点,类似于[]string{"a"}
			// 从每个 src nodes中迁移slot(从前往后遍历 src.slots),最大迁移numSlots个slot, 每个slot构成一个迁移任务
			reshardTable := computeReshardTable(srcs, int(numSlots))
			if len(reshardTable) != int(numSlots) {
				log.Error(nil, "*** Assertion failed: Reshard table != number of slots", "table", len(reshardTable), "slots", numSlots)
			}
			for _, e := range reshardTable {
				if err := c.moveSlot(e, dst, admin); err != nil {
					return err
				}
			}
		}

		// Update nodes balance.
		// log.V(4).Info("balance", "dst", dst.Balance(), "src", src.Balance(), "slots", numSlots)
		dst.SetBalance(dst.Balance() + int(numSlots))
		src.SetBalance(src.Balance() - int(numSlots))
		log.Info(fmt.Sprintf("src:%s src.balance:%d,dst:%s dst.balance:%d", src.PodName, src.Balance(), dst.PodName, dst.Balance()))
		if dst.Balance() == 0 {
			dstIdx += 1
		}
		if src.Balance() == 0 {
			srcIdx -= 1
		}
	}

	return nil
}

type MovedNode struct {
	Source *redisutil.Node
	Slot   redisutil.Slot
}

// computeReshardTable Given a list of source nodes return a "resharding plan"
// with what slots to move in order to move "numslots" slots to another instance.
// params:
// - src: 迁移的源节点;
// - numSlots: 需迁移的slots个数
// 从每个 src nodes中迁移slot(从前往后遍历 src.slots),最大迁移numSlots个slot, 每个slot构成一个迁移任务
func computeReshardTable(src redisutil.Nodes, numSlots int) []*MovedNode {
	var moved []*MovedNode

	sources := src.SortByFunc(func(a, b *redisutil.Node) bool { return a.TotalSlots() < b.TotalSlots() }) //将src node按照所负责slot个数从小到大排序
	//src nodes负责的slot总个数
	sourceTotSlots := 0
	for _, node := range sources {
		sourceTotSlots += node.TotalSlots()
	}
	for idx, node := range sources {
		//每个src node迁一点slot, 就能把完成numSlot个迁移目标
		//这里就是计算每个src nodee应该迁移多少,计算方法: 目标迁移slot个数/所有src节点总slot个数*我有多少个slot
		//当src只有一个节点时, 所有src节点总slot个数==我有多少个slot, 所以 n == numSlots
		n := float64(numSlots) / float64(sourceTotSlots) * float64(node.TotalSlots())

		if idx == 0 {
			n = math.Ceil(n) // 向上取整, 当 src中只有一个节点时 无所谓
		} else {
			n = math.Floor(n) // 向下取整
		}

		keys := node.Slots

		for i := 0; i < int(n); i++ {
			if len(moved) < numSlots {
				mnode := &MovedNode{
					Source: node,
					Slot:   keys[i], //从每个src node前面的slot开始迁移,每个slot构成一个迁移任务
				}
				moved = append(moved, mnode)
			}
		}
	}
	return moved
}

// 迁移slot
func (c *Ctx) moveSlot(source *MovedNode, target *redisutil.Node, admin redisutil.IAdmin) error {
	if err := admin.SetSlot(target.IPPort(), "IMPORTING", source.Slot, target.ID); err != nil {
		return err
	}
	if err := admin.SetSlot(source.Source.IPPort(), "MIGRATING", source.Slot, source.Source.ID); err != nil {
		return err
	}
	if _, err := admin.MigrateKeysInSlot(source.Source.IPPort(), target, source.Slot, 10, 30000, true); err != nil {
		return err
	}
	if err := admin.SetSlot(target.IPPort(), "NODE", source.Slot, target.ID); err != nil {
		c.log.Error(err, "SET NODE", "node", target.IPPort())
	}
	if err := admin.SetSlot(source.Source.IPPort(), "NODE", source.Slot, target.ID); err != nil {
		c.log.Error(err, "SET NODE", "node", source.Source.IPPort())
	}
	source.Source.Slots = redisutil.RemoveSlot(source.Source.Slots, source.Slot)
	return nil
}

//AllocSlots  将16384个slot在集群中分配
func (c *Ctx) AllocSlots(admin redisutil.IAdmin, newMasterNodes redisutil.Nodes) error {
	mastersNum := len(newMasterNodes)
	clusterHashSlots := int(admin.GetHashMaxSlot() + 1)
	slotsPerNode := float64(clusterHashSlots) / float64(mastersNum)
	first := 0
	cursor := 0.0
	for index, node := range newMasterNodes { //按照 stateFulSet index排序, -0、-1、-2、-3 排序
		last := utils.Round(cursor + slotsPerNode - 1) //四舍五入
		if last > clusterHashSlots || index == mastersNum-1 {
			last = clusterHashSlots - 1
		}

		if last < first {
			last = first
		}

		node.Slots = redisutil.BuildSlotSlice(redisutil.Slot(first), redisutil.Slot(last)) //[first,last]间的slot列表
		first = last + 1
		cursor += slotsPerNode
		c.log.Info(fmt.Sprintf("AllocSlots node:%s slots:%s", node.PodName, ConvertSlotToShellFormat(node.Slots)))
		if err := admin.AddSlots(node.IPPort(), node.Slots); err != nil {
			return err
		}
	}
	return nil
}
