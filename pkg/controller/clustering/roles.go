package clustering

import (
	"fmt"

	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
)

// AttachingSlavesToMaster used to attach slaves to there masters
// 遍历 slavesByMaster, 将每个slave与其master建立联系
func (c *Ctx) AttachingSlavesToMaster(admin redisutil.IAdmin) error {
	var globalErr error
	for masterID, slaves := range c.slavesByMaster {
		masterNode, err := c.cluster.GetNodeByID(masterID)
		if err != nil {
			c.log.Error(err, fmt.Sprintf("unable fo found the Cluster.Node with redis ID:%s", masterID))
			continue
		}
		for _, slave := range slaves {
			c.log.Info(fmt.Sprintf("attaching node %s to master %s", slave.ID, masterID))
			c.log.Info(fmt.Sprintf("attaching node %s to master %s", slave.PodName, masterNode.PodName))

			//- slave执行cluster replicate $masterID
			//- slave设置自己的MasterReferent=masterID, slave.Role=slave;
			err := admin.AttachSlaveToMaster(slave, masterNode.ID)
			if err != nil {
				c.log.Error(err, fmt.Sprintf("attaching node %s to master %s", slave.ID, masterID))
				globalErr = err
			}
		}
	}
	return globalErr
}
