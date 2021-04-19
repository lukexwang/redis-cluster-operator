package clustering

import (
	"fmt"
	"math"
	"sort"
	"strings"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
)

var log = logf.Log.WithName("clustering")

type migrationInfo struct {
	From *redisutil.Node
	To   *redisutil.Node
}

type mapSlotByMigInfo map[migrationInfo][]redisutil.Slot

// DispatchMasters used to select nodes with master roles
func DispatchMasters(cluster *redisutil.Cluster, nodes redisutil.Nodes, nbMaster int32) (redisutil.Nodes, redisutil.Nodes, redisutil.Nodes, error) {
	log.Info("start dispatching slots to masters", "nb nodes:", len(nodes))
	var allMasterNodes redisutil.Nodes
	// First loop get Master with already Slots assign on it
	currentMasterNodes := nodes.FilterByFunc(redisutil.IsMasterWithSlot)
	allMasterNodes = append(allMasterNodes, currentMasterNodes...)

	// add also available Master without slot
	currentMasterWithNoSlot := nodes.FilterByFunc(redisutil.IsMasterWithNoSlot)
	allMasterNodes = append(allMasterNodes, currentMasterWithNoSlot...)
	log.V(2).Info("master with no slot", "lens:", len(currentMasterWithNoSlot))

	newMasterNodesSmartSelection, besteffort, err := PlaceMasters(cluster, currentMasterNodes, currentMasterWithNoSlot, nbMaster)

	log.V(2).Info(fmt.Sprintf("Total masters: %d - target %d - selected: %d", len(allMasterNodes), nbMaster, len(newMasterNodesSmartSelection)))
	if err != nil {
		return redisutil.Nodes{}, redisutil.Nodes{}, redisutil.Nodes{}, fmt.Errorf("not enough master available current:%d target:%d, err:%v", len(allMasterNodes), nbMaster, err)
	}

	newMasterNodesSmartSelection = newMasterNodesSmartSelection.SortByFunc(func(a, b *redisutil.Node) bool { return a.ID < b.ID })

	cluster.Status = redisv1alpha1.ClusterStatusCalculatingRebalancing
	if besteffort {
		cluster.NodesPlacement = redisv1alpha1.NodesPlacementInfoBestEffort
	} else {
		cluster.NodesPlacement = redisv1alpha1.NodesPlacementInfoOptimal
	}

	return newMasterNodesSmartSelection, currentMasterNodes, allMasterNodes, nil
}

// DispatchSlotToNewMasters used to dispatch Slot to the new master nodes
//- 以缩容的角度来看,newMasterNodes 保存的是(缩容后保留的)的stateFulSet 对应的master. newMasterNodes 应该换个名称 expectedMasterNodes
//- curMasterNodes 保存的是当前集群中所有(负责slot的)master, currentMasterNodes 应该换个名称 currentSlotMasterNodes
//- 获取缩容的 slot搬迁计划,执行slot搬迁;
func (c *Ctx) DispatchSlotToNewMasters(admin redisutil.IAdmin, newMasterNodes, currentMasterNodes, allMasterNodes redisutil.Nodes) error {
	// Calculate the Migration slot information (which slots goes from where to where)
	migrationSlotInfo, info := c.feedMigInfo(newMasterNodes, currentMasterNodes, allMasterNodes, int(admin.GetHashMaxSlot()+1)) //admin.GetHashMaxSlot+1 ==> 16384
	c.cluster.ActionsInfo = info
	c.cluster.Status = redisv1alpha1.ClusterStatusRebalancing
	for nodesInfo, slots := range migrationSlotInfo {
		// There is a need for real error handling here, we must ensure we don't keep a slot in abnormal state
		if nodesInfo.From == nil {
			c.log.V(4).Info("1) add slots that having probably been lost during scale down", "destination:", nodesInfo.To.ID, "total:", len(slots), " : ", redisutil.SlotSlice(slots))
			err := admin.AddSlots(nodesInfo.To.IPPort(), slots)
			if err != nil {
				c.log.Error(err, "error during ADDSLOTS")
				return err
			}
		} else {
			c.log.V(6).Info("1) Send SETSLOT IMPORTING command", "target:", nodesInfo.To.ID, "source-node:", nodesInfo.From.ID, " total:", len(slots), " : ", redisutil.SlotSlice(slots))
			err := admin.SetSlots(nodesInfo.To.IPPort(), "IMPORTING", slots, nodesInfo.From.ID)
			if err != nil {
				c.log.Error(err, "error during IMPORTING")
				return err
			}
			c.log.V(6).Info("2) Send SETSLOT MIGRATION command", "target:", nodesInfo.From.ID, "destination-node:", nodesInfo.To.ID, " total:", len(slots), " : ", redisutil.SlotSlice(slots))
			err = admin.SetSlots(nodesInfo.From.IPPort(), "MIGRATING", slots, nodesInfo.To.ID)
			if err != nil {
				c.log.Error(err, "error during MIGRATING")
				return err
			}

			c.log.V(6).Info("3) Migrate Key")
			nbMigrated, migerr := admin.MigrateKeys(nodesInfo.From.IPPort(), nodesInfo.To, slots, 10, 30000, true)
			if migerr != nil {
				c.log.Error(migerr, "error during MIGRATION")
			} else {
				c.log.V(7).Info("migrated", "key", nbMigrated)
			}

			// we absolutly need to do setslot on the node owning the slot first, otherwise in case of manager crash, only the owner may think it is now owning the slot
			// creating a cluster view discrepency
			err = admin.SetSlots(nodesInfo.To.IPPort(), "NODE", slots, nodesInfo.To.ID)
			if err != nil {
				c.log.V(4).Info(fmt.Sprintf("warning during SETSLOT NODE on %s: %v", nodesInfo.To.IPPort(), err))
			}
			err = admin.SetSlots(nodesInfo.From.IPPort(), "NODE", slots, nodesInfo.To.ID)
			if err != nil {
				c.log.V(4).Info(fmt.Sprintf("warning during SETSLOT NODE on %s: %v", nodesInfo.From.IPPort(), err))
			}

			// Update bom
			nodesInfo.From.Slots = redisutil.RemoveSlots(nodesInfo.From.Slots, slots)

			// now tell all other nodes
			for _, master := range allMasterNodes {
				if master.IPPort() == nodesInfo.To.IPPort() || master.IPPort() == nodesInfo.From.IPPort() {
					// we already did those two
					continue
				}
				if master.TotalSlots() == 0 {
					// some nodes may not be master anymore
					// as we removed all slots in previous iteration of this code
					// we ignore those nodes
					continue
				}
				c.log.V(6).Info("4) Send SETSLOT NODE command", "target:", master.ID, "new owner:", nodesInfo.To.ID, " total:", len(slots), " : ", redisutil.SlotSlice(slots))
				err = admin.SetSlots(master.IPPort(), "NODE", slots, nodesInfo.To.ID)
				if err != nil {
					c.log.V(4).Info(fmt.Sprintf("warning during SETSLOT NODE on %s: %v", master.IPPort(), err))
				}
			}
		}
		// Update bom
		nodesInfo.To.Slots = redisutil.AddSlots(nodesInfo.To.Slots, slots)
	}
	return nil
}

//feedMigInfo 生成迁移计划,最后的返回结果中:
// mapOut: 保存迁移计划,key中包含 from、to,value中是需要迁移的slot列表
func (c *Ctx) feedMigInfo(newMasterNodes, oldMasterNodes, allMasterNodes redisutil.Nodes, nbSlots int) (mapOut mapSlotByMigInfo, info redisutil.ClusterActionsInfo) {
	mapOut = make(mapSlotByMigInfo) //保存迁移计划,key中包含 from、to,value中是需要迁移的slot
	/*
		mapSlotToUpdate
		每个节点需要 迁移入(importing) 这些slot
		slotToAddBymaster => {
		  "master01"=>[10924..12288],
		  "master02"=>[12289..13653],
		  "master03"=>[13654,13655..15018],
		  "master04"=>[15019..16383]
		}
	*/
	mapSlotToUpdate := c.buildSlotsByNode(newMasterNodes, oldMasterNodes, allMasterNodes, nbSlots)

	for id, slots := range mapSlotToUpdate {
		for _, s := range slots {
			found := false
			for _, oldNode := range oldMasterNodes {
				if oldNode.ID == id {
					if redisutil.Contains(oldNode.Slots, s) { // slot迁移的目标节点就是 '我',而且'我'现在已经负责该slot了,所以这个slot不用处理了,break
						found = true
						break
					}
					continue // slot迁移的目标节点就是 '我', '我'现在没负责该slot,继续找下一个负责该slot的节点罗,continue
				}
				if redisutil.Contains(oldNode.Slots, s) { //找到了负责该slot的真正节点,那么就可以生成迁移计划
					newNode, err := newMasterNodes.GetNodeByID(id)
					if err != nil {
						c.log.Error(err, "unable to find node", "with id:", id)
						continue
					}
					mapOut[migrationInfo{From: oldNode, To: newNode}] = append(mapOut[migrationInfo{From: oldNode, To: newNode}], s)
					found = true
					// increment slots counter
					info.NbslotsToMigrate++
					break
				}
			}
			if !found { // 没有找到负责该slot的节点
				// new slots added (not from an existing master). Correspond to lost slots during important scale down
				newNode, err := newMasterNodes.GetNodeByID(id)
				if err != nil {
					c.log.Error(err, "unable to find node", "with id:", id)
					continue
				}
				// 对于没有节点负责的slot,直接把该slot利用 cluster addslot 方式加入到 newNode 中
				mapOut[migrationInfo{From: nil, To: newNode}] = append(mapOut[migrationInfo{From: nil, To: newNode}], s)

				// increment slots counter
				info.NbslotsToMigrate++
			}
		}
	}
	return mapOut, info
}

// buildSlotsByNode get all slots that have to be migrated with retrieveSlotToMigrateFrom and retrieveSlotToMigrateFromRemovedNodes
// and assign those slots to node that need them
// 通过 retrieveSlotToMigrateFrom 和 retrieveSlotToMigrateFromRemovedNodes 获取所有需要迁移的slot
// 以及将这些slot赋值给 需他们的node
func (c *Ctx) buildSlotsByNode(newMasterNodes, oldMasterNodes, allMasterNodes redisutil.Nodes, nbSlots int) map[string][]redisutil.Slot {
	var nbNode = len(newMasterNodes)
	if nbNode == 0 {
		return make(map[string][]redisutil.Slot)
	}
	nbSlotByNode := int(math.Ceil(float64(nbSlots) / float64(nbNode)))               // 缩容后目标master 每个应该有多少slot
	slotToMigrateByNode := c.retrieveSlotToMigrateFrom(oldMasterNodes, nbSlotByNode) // 如果某个master上slot过多 大于 nbSlotByNode,生成迁移计划
	// retrieveSlotToMigrateFromRemovedNodes
	// - 存在oldMasterNodes中,但不存在 newMasterNodes中的节点就是需要removed的node=>removedNodes;
	// - 返回需要removed的node 的迁移计划, map[string]slots, key: nodeID, value: slots;
	slotToMigrateByNodeFromDeleted := c.retrieveSlotToMigrateFromRemovedNodes(newMasterNodes, oldMasterNodes)
	for id, slots := range slotToMigrateByNodeFromDeleted {
		slotToMigrateByNode[id] = slots
	}

	// 找出[0,nbSlots) 之间, 没有任何node负责的slot 有哪些
	slotToMigrateByNode[""] = c.retrieveLostSlots(oldMasterNodes, nbSlots)
	if len(slotToMigrateByNode[""]) != 0 {
		c.log.Error(nil, fmt.Sprintf("several slots have been lost: %v", redisutil.SlotSlice(slotToMigrateByNode[""])))
	}
	slotToAddByNode := c.buildSlotByNodeFromAvailableSlots(newMasterNodes, nbSlotByNode, slotToMigrateByNode)

	total := 0
	for _, node := range allMasterNodes {
		//这里的for循环目的是为了打印下日志
		currentSlots := 0
		removedSlots := 0
		addedSlots := 0
		expectedSlots := 0
		if slots, ok := slotToMigrateByNode[node.ID]; ok {
			removedSlots = len(slots)
		}
		if slots, ok := slotToAddByNode[node.ID]; ok {
			addedSlots = len(slots)
		}
		currentSlots += len(node.Slots)
		total += currentSlots - removedSlots + addedSlots
		searchByAddrFunc := func(n *redisutil.Node) bool {
			return n.IPPort() == node.IPPort()
		}
		if _, err := newMasterNodes.GetNodesByFunc(searchByAddrFunc); err == nil {
			expectedSlots = nbSlotByNode
		}
		c.log.Info(fmt.Sprintf("node %s will have %d + %d - %d = %d slots; expected: %d[+/-%d]", node.ID, currentSlots, addedSlots, removedSlots, currentSlots+addedSlots-removedSlots, expectedSlots, len(newMasterNodes)))
	}
	c.log.Info(fmt.Sprintf("Total slots: %d - expected: %d", total, nbSlots))

	return slotToAddByNode
}

// retrieveSlotToMigrateFrom list the number of slots that need to be migrated to reach nbSlotByNode per nodes
// 列出每个node需要迁移到 多少个 slot, 才能达到 负责slot数=nbSlotByNode
func (c *Ctx) retrieveSlotToMigrateFrom(oldMasterNodes redisutil.Nodes, nbSlotByNode int) map[string][]redisutil.Slot {
	slotToMigrateByNode := make(map[string][]redisutil.Slot)
	for _, node := range oldMasterNodes {
		c.log.V(6).Info("--- oldMasterNode:", "ID:", node.ID)
		nbSlot := node.TotalSlots()
		if nbSlot >= nbSlotByNode { //当前节点的slot个数大于 nbSlotByNode,则当前节点需要将多余的slot迁走
			if len(node.Slots[nbSlotByNode:]) > 0 {
				slotToMigrateByNode[node.ID] = append(slotToMigrateByNode[node.ID], node.Slots[nbSlotByNode:]...)
			}
			c.log.V(6).Info(fmt.Sprintf("--- migrating from %s, %d slots", node.ID, len(slotToMigrateByNode[node.ID])))
		}
	}
	return slotToMigrateByNode
}

// retrieveSlotToMigrateFromRemovedNodes given the list of node that will be masters with slots, and the list of nodes that were masters with slots
// return the list of slots from previous nodes that will be moved, because this node will no longer hold slots
// - 存在oldMasterNodes中,但不存在 newMasterNodes中的节点就是需要removed的node=>removedNodes;
// - 返回需要removed的node 的迁移计划, map[string]slots, key: nodeID, value: slots;
func (c *Ctx) retrieveSlotToMigrateFromRemovedNodes(newMasterNodes, oldMasterNodes redisutil.Nodes) map[string][]redisutil.Slot {
	slotToMigrateByNode := make(map[string][]redisutil.Slot)
	var removedNodes redisutil.Nodes
	for _, old := range oldMasterNodes {
		c.log.V(6).Info("--- oldMasterNode:", old.ID)
		isPresent := false //存在oldMasterNodes中,但不存在 newMasterNodes中的节点就是需要removed的node
		for _, new := range newMasterNodes {
			if old.ID == new.ID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			removedNodes = append(removedNodes, old)
		}
	}

	for _, node := range removedNodes {
		slotToMigrateByNode[node.ID] = node.Slots
		c.log.V(6).Info(fmt.Sprintf("--- migrating from %s, %d slots", node.ID, len(slotToMigrateByNode[node.ID])))
	}
	return slotToMigrateByNode
}

// retrieveLostSlots retrieve the list of slots that are not attributed to a node
// 找出[0,nbSlots) 之间, 没有任何node负责的slot 有哪些
func (c *Ctx) retrieveLostSlots(oldMasterNodes redisutil.Nodes, nbSlots int) []redisutil.Slot {
	currentFullRange := []redisutil.Slot{} //当前master负责的全部slot
	for _, node := range oldMasterNodes {
		// TODO a lot of perf improvement can be done here with better algorithm to add slot ranges
		currentFullRange = append(currentFullRange, node.Slots...)
	}
	sort.Sort(redisutil.SlotSlice(currentFullRange))
	lostSlots := []redisutil.Slot{}
	// building []slot of slots that are missing from currentFullRange to reach [0, nbSlots]
	last := redisutil.Slot(0)
	if len(currentFullRange) == 0 || currentFullRange[0] != 0 {
		lostSlots = append(lostSlots, 0)
	}
	for _, slot := range currentFullRange {
		if slot > last+1 {
			for i := last + 1; i < slot; i++ {
				lostSlots = append(lostSlots, i)
			}
		}
		last = slot
	}
	for i := last + 1; i < redisutil.Slot(nbSlots); i++ {
		lostSlots = append(lostSlots, i)
	}

	return lostSlots
}

//buildSlotByNodeFromAvailableSlots
//params:
// - newMasterNodes: 缩容后目标节点;
// - nbSlotByNode: 缩容后每个目标节点 期望有多少 slot;
// - slotToMigrateByNode: 被缩容掉的node,这些node对应能迁移出的slot详情;
/*
缩容计划示例:
比如当前共有6个master负责 16384 个slot:
master01 slot: 0-2730      个数: 2731
master02 slot: 2731-5461   个数: 2731
master03 slot: 5462-8192   个数: 2731
master04 slot: 8193-10923  个数: 2731
master05 slot: 10924-13654 个数: 2731
master06 slot: 13655-16383 个数: 2729

现在 6 缩 4，缩容后目标节点负责slot数 16384/4=4096 (4096-2731=1375)


slotToMigrateBymaster=>{
  "master05" => [10924..13654],
  "master06" => [13655..16383]
}

每个节点需要迁移入这些slot
slotToAddBymaster => {
  "master01"=>[10924..12288],
  "master02"=>[12289..13653],
  "master03"=>[13654,13655..15018],
  "master04"=>[15019..16383]
}
迁移计划:
master05 => master01, 迁移slot: 10924..12288
master05 => master02, 迁移slot: 12289..13653
master05 => master03, 迁移slot: 13654
master06 => master04, 迁移slot: 13655..15018
master06 => master04, 迁移slot: 15019..16383

最终结果:
"master01" => [0..2730,10924..12288],
"master02" => [2731..5461,12289..13653],
"master03" => [5462..8192,13654..15018]
"master04" => [13655..16383,15019..16383]
*/
func (c *Ctx) buildSlotByNodeFromAvailableSlots(newMasterNodes redisutil.Nodes, nbSlotByNode int, slotToMigrateByNode map[string][]redisutil.Slot) map[string][]redisutil.Slot {
	slotToAddByNode := make(map[string][]redisutil.Slot)
	slotNodeForLog := make(map[string][]redisutil.Slot)
	var nbNode = len(newMasterNodes)
	if nbNode == 0 {
		return slotToAddByNode
	}
	var slotOfNode = make(map[int][]redisutil.Slot)
	for i, node := range newMasterNodes {
		slotOfNode[i] = node.Slots
	}
	var idNode = 0
	for _, slotsFrom := range slotToMigrateByNode {
		for _, slot := range slotsFrom {
			var missingSlots = nbSlotByNode - len(slotOfNode[idNode])
			if missingSlots > 0 {
				slotOfNode[idNode] = append(slotOfNode[idNode], slot)
				slotToAddByNode[newMasterNodes[idNode].ID] = append(slotToAddByNode[newMasterNodes[idNode].ID], slot)
				slotNodeForLog[newMasterNodes[idNode].PodName] = append(slotNodeForLog[newMasterNodes[idNode].PodName], slot)
			} else {
				idNode++
				if idNode > (nbNode - 1) {
					// all nodes have been filled
					// 所有 缩容后目标节点 都填满了,如果还有slot待迁移,就全部迁移到 '最后一个' node上
					idNode-- //上面 ++ 这里 --, 意思是继续将slot 放到last node上
					c.log.V(7).Info(fmt.Sprintf("some available slots have not been assigned, over-filling node %s", newMasterNodes[idNode].ID))
				}
				slotOfNode[idNode] = append(slotOfNode[idNode], slot)
				slotToAddByNode[newMasterNodes[idNode].ID] = append(slotToAddByNode[newMasterNodes[idNode].ID], slot)
				slotNodeForLog[newMasterNodes[idNode].PodName] = append(slotNodeForLog[newMasterNodes[idNode].PodName], slot)
			}
		}
	}
	for podName, slots := range slotNodeForLog {
		log.Info(fmt.Sprintf("buildSlotByNodeFromAvailableSlots podname=>%s balance:%s", podName, ConvertSlotToShellFormat(slots)))
	}

	return slotToAddByNode
}

//ConvertSlotToShellFormat 将slots:[0,1,2,3,4,10,11,12,13,17] 按照 {0..4} {10..13} 17 打印
func ConvertSlotToShellFormat(slots []redisutil.Slot) string {
	if len(slots) == 0 {
		return ""
	}
	str01 := ""
	start := slots[0]
	curr := slots[0]
	for _, item := range slots {
		next := item
		if next == curr {
			continue
		}
		if curr == next-1 {
			// slot连续,继续
			curr = next
			continue
		}
		//slot不连续了
		if start == curr {
			str01 = fmt.Sprintf("%s %d", str01, start)
		} else {
			str01 = fmt.Sprintf("%s {%d..%d}", str01, start, curr)
		}
		start = next
		curr = next
	}
	// 最后再处理一次start curr
	if start == curr {
		str01 = fmt.Sprintf("%s %d", str01, start)
	} else {
		str01 = fmt.Sprintf("%s {%d..%d}", str01, start, curr)
	}
	str01 = strings.Trim(str01, " ")
	return str01
}
