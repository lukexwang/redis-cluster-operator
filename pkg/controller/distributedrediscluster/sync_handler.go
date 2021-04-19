package distributedrediscluster

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/config"
	"github.com/ucloud/redis-cluster-operator/pkg/controller/clustering"
	"github.com/ucloud/redis-cluster-operator/pkg/controller/manager"
	"github.com/ucloud/redis-cluster-operator/pkg/k8sutil"
	"github.com/ucloud/redis-cluster-operator/pkg/redisutil"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/statefulsets"
)

const (
	requeueAfter = 10 * time.Second
)

type syncContext struct {
	cluster      *redisv1alpha1.DistributedRedisCluster
	clusterInfos *redisutil.ClusterInfos
	admin        redisutil.IAdmin
	healer       manager.IHeal
	pods         []*corev1.Pod
	reqLogger    logr.Logger
}

//ensureCluster
//- 恢复数据时, 将appendonly 设置为no;
//- 生成or更新redisCluster configmap: redis-cluster-${cluster_name}、恢复数据时configmap: rediscluster-restore-${clusterName}
//- 如果密码发生改变,则修改redis密码
//- (更新/创建) cluster stateFulSet(数据恢复步骤在initContainer中);
//- (更新/创建) service,每个stateFulSet一个headless service, cluster有一个ClusterIP类型的service;
func (r *ReconcileDistributedRedisCluster) ensureCluster(ctx *syncContext) error {
	cluster := ctx.cluster
	if err := r.validateAndSetDefault(cluster, ctx.reqLogger); err != nil {
		if k8sutil.IsRequestRetryable(err) {
			return Kubernetes.Wrap(err, "Validate")
		}
		return StopRetry.Wrap(err, "stop retry")
	}

	// Redis only load db from append only file when AOF ON, because of
	// we only backed up the RDB file when doing data backup, so we set
	// "appendonly no" force here when do restore.
	// 恢复数据时, 将appendonly 设置为no, 因为我们的备份只会备份 RDB 文件
	dbLoadedFromDiskWhenRestore(cluster, ctx.reqLogger)
	labels := getLabels(cluster)
	// EnsureRedisConfigMap 确认configmap的内容
	// - 为redis-cluster生成or更新configmap: redis-cluster-${cluster_name},configmap中包含 shutdown.sh、fix-ip.sh、redis.conf等
	// - 如果集群需要从备份中恢复数据,则为resotre生成configmap, 名字: rediscluster-restore-${clusterName}
	if err := r.ensurer.EnsureRedisConfigMap(cluster, labels); err != nil {
		return Kubernetes.Wrap(err, "EnsureRedisConfigMap")
	}

	//如果密码发生改变,则重置所有redis pod的密码为新密码
	if err := r.resetClusterPassword(ctx); err != nil {
		return Cluster.Wrap(err, "ResetPassword")
	}

	if updated, err := r.ensurer.EnsureRedisStatefulsets(cluster, labels); err != nil {
		ctx.reqLogger.Error(err, "EnsureRedisStatefulSets")
		return Kubernetes.Wrap(err, "EnsureRedisStatefulSets")
	} else if updated {
		// update cluster status = RollingUpdate immediately when cluster's image or resource or password changed
		// 设置 cluster.Status 为 RollingUpdate
		SetClusterUpdating(&cluster.Status, "cluster spec updated")
		r.crController.UpdateCRStatus(cluster)
		waiter := &waitStatefulSetUpdating{
			name:                  "waitStatefulSetUpdating",
			timeout:               30 * time.Second * time.Duration(cluster.Spec.ClusterReplicas+2),
			tick:                  5 * time.Second,
			statefulSetController: r.statefulSetController,
			cluster:               cluster,
		}
		if err := waiting(waiter, ctx.reqLogger); err != nil {
			return err
		}
	}
	//为每个stateFulSet 创建 headLessSvcs
	if err := r.ensurer.EnsureRedisHeadLessSvcs(cluster, labels); err != nil {
		return Kubernetes.Wrap(err, "EnsureRedisHeadLessSvcs")
	}
	//为Cluster创建一个 service:ClusterIP
	if err := r.ensurer.EnsureRedisSvc(cluster, labels); err != nil {
		return Kubernetes.Wrap(err, "EnsureRedisSvc")
	}
	// EnsureRedisRCloneSecret 不太清楚RCloneSecret是用来干啥的
	if err := r.ensurer.EnsureRedisRCloneSecret(cluster, labels); err != nil {
		if k8sutil.IsRequestRetryable(err) {
			return Kubernetes.Wrap(err, "EnsureRedisRCloneSecret")
		}
		return StopRetry.Wrap(err, "stop retry")
	}
	return nil
}

//waitPodReady 删除处于terminating状态的pod,并检查每个redis的stateFulSet对应pod数是否符合预期,不符合预期报错
func (r *ReconcileDistributedRedisCluster) waitPodReady(ctx *syncContext) error {
	if _, err := ctx.healer.FixTerminatingPods(ctx.cluster, 5*time.Minute); err != nil {
		return Kubernetes.Wrap(err, "FixTerminatingPods")
	}
	//获取每个redis的statefulSet信息,并检查statefulSet对应的pod个数是否符合预期,不符合预期则报错
	if err := r.checker.CheckRedisNodeNum(ctx.cluster); err != nil {
		return Requeue.Wrap(err, "CheckRedisNodeNum")
	}

	return nil
}

//validateAndSetDefault 验证参数的有效性 并对 某些cluster.Spec设置默认值(比如是否需要从备份恢复数据、cluster.spec.image、cluster.spec.maxsize等)
func (r *ReconcileDistributedRedisCluster) validateAndSetDefault(cluster *redisv1alpha1.DistributedRedisCluster, reqLogger logr.Logger) error {
	var update bool
	var err error

	if cluster.IsRestoreFromBackup() && cluster.ShouldInitRestorePhase() {
		//需要从备份中恢复数据,初始化 cluster.status.restore 信息
		update, err = r.initRestore(cluster, reqLogger)
		if err != nil {
			return err
		}
	}

	if cluster.IsRestoreFromBackup() && (cluster.IsRestoreRunning() || cluster.IsRestoreRestarting()) {
		// Set ClusterReplicas = 0, only start master node in first reconcile loop when do restore
		// 在恢复数据的第一次reconcile,只启动master节点(不启动slave),所以设置 ClusterReplicas=0
		cluster.Spec.ClusterReplicas = 0
	}

	updateDefault := cluster.DefaultSpec(reqLogger)
	if update || updateDefault {
		//如果cluster的值被更新过,则update
		return r.crController.UpdateCR(cluster)
	}

	return nil
}

//dbLoadedFromDiskWhenRestore 数据恢复时, 将appendonly 设置为 no
func dbLoadedFromDiskWhenRestore(cluster *redisv1alpha1.DistributedRedisCluster, reqLogger logr.Logger) {
	if cluster.IsRestoreFromBackup() && !cluster.IsRestored() {
		if cluster.Spec.Config != nil {
			reqLogger.Info("force appendonly = no when do restore")
			cluster.Spec.Config["appendonly"] = "no"
		}
	}
}

//initRestore 初始化集群的 restore 信息,restore对应的backup、phase(状态)信息,同时可以从备份中得到 当时的镜像、集群节点个数等信息
func (r *ReconcileDistributedRedisCluster) initRestore(cluster *redisv1alpha1.DistributedRedisCluster, reqLogger logr.Logger) (bool, error) {
	update := false
	if cluster.Status.Restore.Backup == nil {
		initSpec := cluster.Spec.Init
		backup, err := r.crController.GetRedisClusterBackup(initSpec.BackupSource.Namespace, initSpec.BackupSource.Name)
		if err != nil {
			reqLogger.Error(err, "GetRedisClusterBackup")
			return update, err
		}
		if backup.Status.Phase != redisv1alpha1.BackupPhaseSucceeded {
			reqLogger.Error(nil, "backup is still running")
			return update, fmt.Errorf("backup is still running")
		}
		//到这里,说明上一次备份状态为success
		//修改restore.Phase == running, 代表'恢复中'
		cluster.Status.Restore.Backup = backup
		cluster.Status.Restore.Phase = redisv1alpha1.RestorePhaseRunning
		if err := r.crController.UpdateCRStatus(cluster); err != nil {
			return update, err
		}
	}
	//从备份中拿到当时的 镜像信息 和 集群大小信息
	backup := cluster.Status.Restore.Backup
	if cluster.Spec.Image == "" {
		cluster.Spec.Image = backup.Status.ClusterImage
		update = true
	}
	if cluster.Spec.MasterSize != backup.Status.MasterSize {
		cluster.Spec.MasterSize = backup.Status.MasterSize
		update = true
	}

	return update, nil
}

//waitForClusterJoin 所有节点均和firsetNode执行cluster meet, 然后重新获取所有节点各自的 cluster nodes信息
func (r *ReconcileDistributedRedisCluster) waitForClusterJoin(ctx *syncContext) error {
	if infos, err := ctx.admin.GetClusterInfos(); err == nil {
		ctx.reqLogger.V(6).Info("debug waitForClusterJoin", "cluster infos", infos)
		return nil
	}
	var firstNode *redisutil.Node
	for _, nodeInfo := range ctx.clusterInfos.Infos {
		firstNode = nodeInfo.Node
		break
	}
	ctx.reqLogger.Info(">>> Sending CLUSTER MEET messages to join the cluster")
	// 集群所有其他node都cluster meet firstNode, 同时更新Admin.Connections()中 firstNode的connection
	err := ctx.admin.AttachNodeToCluster(firstNode.IPPort())
	if err != nil {
		return Redis.Wrap(err, "AttachNodeToCluster")
	}
	// Give one second for the join to start, in order to avoid that
	// waiting for cluster join will find all the nodes agree about
	// the config as they are still empty with unassigned slots.
	time.Sleep(1 * time.Second)

	_, err = ctx.admin.GetClusterInfos()
	if err != nil {
		return Requeue.Wrap(err, "wait for cluster join")
	}
	return nil
}

// syncCluster
// - 建立集群关系;
// - 完成扩容slot搬迁;
// - 完成缩容slot搬迁,stateFulSet删除;
func (r *ReconcileDistributedRedisCluster) syncCluster(ctx *syncContext) error {
	cluster := ctx.cluster
	admin := ctx.admin
	clusterInfos := ctx.clusterInfos
	expectMasterNum := cluster.Spec.MasterSize
	//获取集群所有信息,包括 clusterName、nameSpace、nodes等信息,nodes中包含了每个node的ID、IP、Port、slot等信息
	rCluster, nodes, err := newRedisCluster(clusterInfos, cluster)
	if err != nil {
		return Cluster.Wrap(err, "newRedisCluster")
	}
	clusterCtx := clustering.NewCtx(rCluster, nodes, cluster.Spec.MasterSize, cluster.Name, ctx.reqLogger)
	//DispatchMasters 调度master
	// - 为每个stateFulSet确定一个redis master节点;保存在 c.newMasterBySts
	// - 对于stateFulSet下没找到任何master的情况,用合适的方法找到一个redis master;
	if err := clusterCtx.DispatchMasters(); err != nil {
		return Cluster.Wrap(err, "DispatchMasters")
	}
	curMasters := clusterCtx.GetCurrentMasters() //负责slot的master
	newMasters := clusterCtx.GetNewMasters()     //所有master(不管是否负责了slot)
	ctx.reqLogger.Info("masters", "newMasters", len(newMasters), "curMasters", len(curMasters))
	if len(curMasters) == 0 {
		//当前没有master,
		ctx.reqLogger.Info("Creating cluster")
		if err := clusterCtx.PlaceSlaves(); err != nil {
			return Cluster.Wrap(err, "PlaceSlaves")

		}
		// 遍历 slavesByMaster, 将每个slave与其master建立联系
		if err := clusterCtx.AttachingSlavesToMaster(admin); err != nil {
			return Cluster.Wrap(err, "AttachingSlavesToMaster")
		}

		//将16384个slot在集群中分配
		if err := clusterCtx.AllocSlots(admin, newMasters); err != nil {
			return Cluster.Wrap(err, "AllocSlots")
		}
	} else if len(newMasters) > len(curMasters) {
		//以扩容角度来看, newMasters中包含了所有stateFulSet对应的master(可能部分master没有负责slot)
		//curMasters保存的是当前集群中所有(负责slot的)master
		ctx.reqLogger.Info("Scaling up") //扩容
		if err := clusterCtx.PlaceSlaves(); err != nil {
			return Cluster.Wrap(err, "PlaceSlaves")

		}
		if err := clusterCtx.AttachingSlavesToMaster(admin); err != nil {
			return Cluster.Wrap(err, "AttachingSlavesToMaster")
		}

		if err := clusterCtx.RebalancedCluster(admin, newMasters); err != nil {
			return Cluster.Wrap(err, "RebalancedCluster")
		}
	} else if cluster.Status.MinReplicationFactor < cluster.Spec.ClusterReplicas {
		ctx.reqLogger.Info("Scaling slave") //存在master slave个数小于 目标 slave个数
		if err := clusterCtx.PlaceSlaves(); err != nil {
			return Cluster.Wrap(err, "PlaceSlaves")

		}
		if err := clusterCtx.AttachingSlavesToMaster(admin); err != nil {
			return Cluster.Wrap(err, "AttachingSlavesToMaster")
		}
	} else if len(curMasters) > int(expectMasterNum) {
		//缩容
		//以缩容的角度来看,newMaster保存的是(缩容后保留的)的stateFulSet 对应的master
		//curMasters保存的是当前集群中所有(负责slot的)master
		ctx.reqLogger.Info("Scaling down")
		var allMaster redisutil.Nodes //allMaster感觉会重复(难道存在stateFulSet0 不负责任何slot,但是缩容需要保留下来)
		allMaster = append(allMaster, newMasters...)
		allMaster = append(allMaster, curMasters...)
		//获取缩容的 slot 搬迁计划,执行slot搬迁
		if err := clusterCtx.DispatchSlotToNewMasters(admin, newMasters, curMasters, allMaster); err != nil {
			return err
		}
		if err := r.scalingDown(ctx, len(curMasters), clusterCtx.GetStatefulsetNodes()); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReconcileDistributedRedisCluster) scalingDown(ctx *syncContext, currentMasterNum int, statefulSetNodes map[string]redisutil.Nodes) error {
	cluster := ctx.cluster
	SetClusterRebalancing(&cluster.Status,
		fmt.Sprintf("scale down, currentMasterSize: %d, expectMasterSize %d", currentMasterNum, cluster.Spec.MasterSize))
	r.crController.UpdateCRStatus(cluster)
	admin := ctx.admin
	expectMasterNum := int(cluster.Spec.MasterSize)
	for i := currentMasterNum - 1; i >= expectMasterNum; i-- {
		stsName := statefulsets.ClusterStatefulSetName(cluster.Name, i)
		for _, node := range statefulSetNodes[stsName] {
			admin.Connections().Remove(node.IPPort()) //断开和这些node的连接
		}
	}
	for i := currentMasterNum - 1; i >= expectMasterNum; i-- {
		stsName := statefulsets.ClusterStatefulSetName(cluster.Name, i)
		ctx.reqLogger.Info("scaling down", "statefulSet", stsName)
		sts, err := r.statefulSetController.GetStatefulSet(cluster.Namespace, stsName)
		if err != nil {
			return Kubernetes.Wrap(err, "GetStatefulSet")
		}
		for _, node := range statefulSetNodes[stsName] {
			//检查stateFulSet下的nodes, 是否还负责 slot,如果负责slot,则直接报错
			ctx.reqLogger.Info("forgetNode", "id", node.ID, "ip", node.IP, "role", node.GetRole())
			if len(node.Slots) > 0 {
				return Redis.New(fmt.Sprintf("node %s is not empty! Reshard data away and try again", node.String()))
			}
			//如果该node也没负责slot了,则forget该node
			if err := admin.ForgetNode(node.ID); err != nil {
				return Redis.Wrap(err, "ForgetNode")
			}
		}
		// remove resource
		//删除stateFulSet
		if err := r.statefulSetController.DeleteStatefulSetByName(cluster.Namespace, stsName); err != nil {
			ctx.reqLogger.Error(err, "DeleteStatefulSetByName", "statefulSet", stsName)
		}
		//删除service
		svcName := statefulsets.ClusterHeadlessSvcName(cluster.Name, i)
		if err := r.serviceController.DeleteServiceByName(cluster.Namespace, svcName); err != nil {
			ctx.reqLogger.Error(err, "DeleteServiceByName", "service", svcName)
		}
		//删除PodDisruptionBudget
		if err := r.pdbController.DeletePodDisruptionBudgetByName(cluster.Namespace, stsName); err != nil {
			ctx.reqLogger.Error(err, "DeletePodDisruptionBudgetByName", "pdb", stsName)
		}
		//删除pvc
		if err := r.pvcController.DeletePvcByLabels(cluster.Namespace, sts.Labels); err != nil {
			ctx.reqLogger.Error(err, "DeletePvcByLabels", "labels", sts.Labels)
		}
		// wait pod Terminating
		waiter := &waitPodTerminating{
			name:                  "waitPodTerminating",
			statefulSet:           stsName,
			timeout:               30 * time.Second * time.Duration(cluster.Spec.ClusterReplicas+2),
			tick:                  5 * time.Second,
			statefulSetController: r.statefulSetController,
			cluster:               cluster,
		}
		if err := waiting(waiter, ctx.reqLogger); err != nil {
			ctx.reqLogger.Error(err, "waitPodTerminating")
		}

	}
	return nil
}

//resetClusterPassword 如果集群密码发生改变,集群密码将重置为新密码
func (r *ReconcileDistributedRedisCluster) resetClusterPassword(ctx *syncContext) error {
	//CheckRedisNodeNum 获取每个redis的statefulSet信息,并检查statefulSet对应的pod个数是否符合预期,不符合预期则报错
	if err := r.checker.CheckRedisNodeNum(ctx.cluster); err == nil {
		namespace := ctx.cluster.Namespace
		name := ctx.cluster.Name
		//获取集群第一个statefulSet
		sts, err := r.statefulSetController.GetStatefulSet(namespace, statefulsets.ClusterStatefulSetName(name, 0))
		if err != nil {
			return err
		}

		//集群密码是否发生改变
		if !statefulsets.IsPasswordChanged(ctx.cluster, sts) {
			return nil
		}

		//设置集群状态为 正在reset密码
		SetClusterResetPassword(&ctx.cluster.Status, "updating cluster's password")
		r.crController.UpdateCRStatus(ctx.cluster)

		//根据labels获取cluster的所有的pods
		matchLabels := getLabels(ctx.cluster)
		redisClusterPods, err := r.statefulSetController.GetStatefulSetPodsByLabels(namespace, matchLabels)
		if err != nil {
			return err
		}

		//旧密码
		oldPassword, err := statefulsets.GetOldRedisClusterPassword(r.client, sts)
		if err != nil {
			return err
		}

		//新密码
		newPassword, err := statefulsets.GetClusterPassword(r.client, ctx.cluster)
		if err != nil {
			return err
		}

		podSet := clusterPods(redisClusterPods.Items)
		//连接集群中的每个redis
		admin, err := newRedisAdmin(podSet, oldPassword, config.RedisConf(), ctx.reqLogger)
		if err != nil {
			return err
		}
		defer admin.Close()

		// Update the password recorded in the file /data/redis_password, redis pod preStop hook
		// need /data/redis_password do CLUSTER FAILOVER
		//newPassword 输入到配置文件中(每个redis pod都会执行)
		cmd := fmt.Sprintf("echo %s > /data/redis_password", newPassword)
		if err := r.execer.ExecCommandInPodSet(podSet, "/bin/sh", "-c", cmd); err != nil {
			return err
		}

		// Reset all redis pod's password.
		//利用config 命令重置所有节点的requirePassword 和 masterAuth
		if err := admin.ResetPassword(newPassword); err != nil {
			return err
		}
	}
	return nil
}
