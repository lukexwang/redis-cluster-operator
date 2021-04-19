package manager

import (
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/k8sutil"
	"github.com/ucloud/redis-cluster-operator/pkg/osm"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/configmaps"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/poddisruptionbudgets"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/services"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/statefulsets"
)

//IEnsureResource 确保RedisStatefulSets、HeadLessSvcs、RedisSvc、RedisConfigMap的正常运行
type IEnsureResource interface {
	EnsureRedisStatefulsets(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) (bool, error)
	EnsureRedisHeadLessSvcs(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error
	EnsureRedisSvc(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error
	EnsureRedisConfigMap(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error
	EnsureRedisRCloneSecret(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error
	UpdateRedisStatefulsets(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error
}

type realEnsureResource struct {
	statefulSetClient k8sutil.IStatefulSetControl
	svcClient         k8sutil.IServiceControl
	configMapClient   k8sutil.IConfigMapControl
	pdbClient         k8sutil.IPodDisruptionBudgetControl
	crClient          k8sutil.ICustomResource
	client            client.Client
	logger            logr.Logger
}

func NewEnsureResource(client client.Client, logger logr.Logger) IEnsureResource {
	return &realEnsureResource{
		statefulSetClient: k8sutil.NewStatefulSetController(client),
		svcClient:         k8sutil.NewServiceController(client),
		configMapClient:   k8sutil.NewConfigMapController(client),
		pdbClient:         k8sutil.NewPodDisruptionBudgetController(client),
		crClient:          k8sutil.NewCRControl(client),
		client:            client,
		logger:            logger,
	}
}

func (r *realEnsureResource) EnsureRedisStatefulsets(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) (bool, error) {
	updated := false
	for i := 0; i < int(cluster.Spec.MasterSize); i++ {
		name := statefulsets.ClusterStatefulSetName(cluster.Name, i)                // such as: drc-example-distributedrediscluster-0
		svcName := statefulsets.ClusterHeadlessSvcName(cluster.Spec.ServiceName, i) // such as: example-distributedrediscluster-0
		// assign label
		labels[redisv1alpha1.StatefulSetLabel] = name
		//ensureRedisStatefulset 更新或创建stateFulSet
		if stsUpdated, err := r.ensureRedisStatefulset(cluster, name, svcName, labels); err != nil {
			return false, err
		} else if stsUpdated {
			updated = stsUpdated
		}
	}
	return updated, nil
}

func (r *realEnsureResource) ensureRedisStatefulset(cluster *redisv1alpha1.DistributedRedisCluster, ssName, svcName string,
	labels map[string]string) (bool, error) {
	//确保Cluster定义了 PodDisruptionBudget(如果没有,则创建)
	//似乎是每个stateFulSet 都对应一个 PDB
	if err := r.ensureRedisPDB(cluster, ssName, labels); err != nil {
		return false, err
	}

	ss, err := r.statefulSetClient.GetStatefulSet(cluster.Namespace, ssName)
	if err == nil {
		//是否应该更新redis stateFulSet, 如镜像、replica、密码、request.Memory、request.CPU等如果发生了改变,就需要更新
		if shouldUpdateRedis(cluster, ss) {
			r.logger.WithValues("StatefulSet.Namespace", cluster.Namespace, "StatefulSet.Name", ssName).
				Info("updating statefulSet")
			//生成stateFulSet的定义,然后走update逻辑
			newSS, err := statefulsets.NewStatefulSetForCR(cluster, ssName, svcName, labels)
			if err != nil {
				return false, err
			}
			return true, r.statefulSetClient.UpdateStatefulSet(newSS)
		}
	} else if err != nil && errors.IsNotFound(err) {
		r.logger.WithValues("StatefulSet.Namespace", cluster.Namespace, "StatefulSet.Name", ssName).
			Info("creating a new statefulSet")
		//生成stateFulSet的定义,然后走create逻辑
		newSS, err := statefulsets.NewStatefulSetForCR(cluster, ssName, svcName, labels)
		if err != nil {
			return false, err
		}
		return false, r.statefulSetClient.CreateStatefulSet(newSS)
	}
	return false, err
}

//shouldUpdateRedis 是否应该更新redis stateFulSet, 如镜像、replica、密码、request.Memory、request.CPU等如果发生了改变,就需要更新
func shouldUpdateRedis(cluster *redisv1alpha1.DistributedRedisCluster, sts *appsv1.StatefulSet) bool {
	if (cluster.Spec.ClusterReplicas + 1) != *sts.Spec.Replicas {
		//ClusterReplicas 是slave的个数,所以 stateFulSet.Spec.Replicas 必须等于 cluster.Spec.ClusterReplicas + 1
		return true
	}
	if cluster.Spec.Image != sts.Spec.Template.Spec.Containers[0].Image {
		return true
	}

	if statefulsets.IsPasswordChanged(cluster, sts) {
		return true
	}

	expectResource := cluster.Spec.Resources
	currentResource := sts.Spec.Template.Spec.Containers[0].Resources
	if result := expectResource.Requests.Memory().Cmp(*currentResource.Requests.Memory()); result != 0 {
		return true
	}
	if result := expectResource.Requests.Cpu().Cmp(*currentResource.Requests.Cpu()); result != 0 {
		return true
	}
	if result := expectResource.Limits.Memory().Cmp(*currentResource.Limits.Memory()); result != 0 {
		return true
	}
	if result := expectResource.Limits.Cpu().Cmp(*currentResource.Limits.Cpu()); result != 0 {
		return true
	}
	return monitorChanged(cluster, sts)
}

func monitorChanged(cluster *redisv1alpha1.DistributedRedisCluster, sts *appsv1.StatefulSet) bool {
	if cluster.Spec.Monitor != nil {
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name == statefulsets.ExporterContainerName {
				return false
			}
		}
		return true
	} else {
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name == statefulsets.ExporterContainerName {
				return true
			}
		}
		return false
	}
}

//ensureRedisPDB 确保Cluster定义了 PodDisruptionBudget(如果没有,则创建)
func (r *realEnsureResource) ensureRedisPDB(cluster *redisv1alpha1.DistributedRedisCluster, name string, labels map[string]string) error {
	_, err := r.pdbClient.GetPodDisruptionBudget(cluster.Namespace, name)
	if err != nil && errors.IsNotFound(err) {
		r.logger.WithValues("PDB.Namespace", cluster.Namespace, "PDB.Name", name).
			Info("creating a new PodDisruptionBudget")
		pdb := poddisruptionbudgets.NewPodDisruptionBudgetForCR(cluster, name, labels)
		return r.pdbClient.CreatePodDisruptionBudget(pdb)
	}
	return err
}

func (r *realEnsureResource) EnsureRedisHeadLessSvcs(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error {
	for i := 0; i < int(cluster.Spec.MasterSize); i++ {
		// 集群每个stateFulSet都对应一个headlessSvc,名称:${serviceName}-${N}
		svcName := statefulsets.ClusterHeadlessSvcName(cluster.Spec.ServiceName, i)
		// 集群statefulSet设置名字: drc-${cluster_name}-${N}
		name := statefulsets.ClusterStatefulSetName(cluster.Name, i)
		// assign label
		labels[redisv1alpha1.StatefulSetLabel] = name
		if err := r.ensureRedisHeadLessSvc(cluster, svcName, labels); err != nil {
			return err
		}
	}
	return nil
}

func (r *realEnsureResource) ensureRedisHeadLessSvc(cluster *redisv1alpha1.DistributedRedisCluster, name string, labels map[string]string) error {
	_, err := r.svcClient.GetService(cluster.Namespace, name)
	if err != nil && errors.IsNotFound(err) {
		r.logger.WithValues("Service.Namespace", cluster.Namespace, "Service.Name", cluster.Spec.ServiceName).
			Info("creating a new headless service")
		svc := services.NewHeadLessSvcForCR(cluster, name, labels)
		return r.svcClient.CreateService(svc)
	}
	return err
}

//EnsureRedisSvc 每个RedisCluster都有一个ClusterIP类型的service
func (r *realEnsureResource) EnsureRedisSvc(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error {
	name := cluster.Spec.ServiceName
	delete(labels, redisv1alpha1.StatefulSetLabel)
	_, err := r.svcClient.GetService(cluster.Namespace, name)
	if err != nil && errors.IsNotFound(err) {
		r.logger.WithValues("Service.Namespace", cluster.Namespace, "Service.Name", cluster.Spec.ServiceName).
			Info("creating a new service")
		//NewSvcForCR 关于cluster的service, 使用默认的 ClusterIP类型,svcNmae名字:{clusterName}
		svc := services.NewSvcForCR(cluster, name, labels)
		return r.svcClient.CreateService(svc)
	}
	return err
}

// EnsureRedisConfigMap 确认configmap的内容
// - 为redis-cluster生成一个新的configmap: redis-cluster-${cluster_name},configmap中包含 shutdown.sh、fix-ip.sh、redis.conf等
// - 如果集群需要从备份中恢复数据,则为resotre生成configmap, 名字: rediscluster-restore-${clusterName}
func (r *realEnsureResource) EnsureRedisConfigMap(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error {
	cmName := configmaps.RedisConfigMapName(cluster.Name)
	drcCm, err := r.configMapClient.GetConfigMap(cluster.Namespace, cmName)
	if err != nil {
		if errors.IsNotFound(err) {
			r.logger.WithValues("ConfigMap.Namespace", cluster.Namespace, "ConfigMap.Name", cmName).
				Info("creating a new configMap")
			cm := configmaps.NewConfigMapForCR(cluster, labels)             //为集群生成一个新的configmap,configmap中包含 shutdown.sh、fix-ip.sh、redis.conf等
			if err2 := r.configMapClient.CreateConfigMap(cm); err2 != nil { //创建configmap
				return err2
			}
		} else {
			return err
		}
	} else {
		//如果redis.conf内容发生了改变,则更新 configmap的内容
		if isRedisConfChanged(drcCm.Data[configmaps.RedisConfKey], cluster.Spec.Config, r.logger) {
			cm := configmaps.NewConfigMapForCR(cluster, labels)
			if err2 := r.configMapClient.UpdateConfigMap(cm); err2 != nil {
				return err2
			}
		}
	}

	if cluster.IsRestoreFromBackup() {
		//如果需要从备份中恢复数据,则为restore生成configmap
		restoreCmName := configmaps.RestoreConfigMapName(cluster.Name)
		restoreCm, err := r.configMapClient.GetConfigMap(cluster.Namespace, restoreCmName)
		if err != nil {
			if errors.IsNotFound(err) {
				r.logger.WithValues("ConfigMap.Namespace", cluster.Namespace, "ConfigMap.Name", restoreCmName).
					Info("creating a new restore configMap")
				cm := configmaps.NewConfigMapForRestore(cluster, labels)
				return r.configMapClient.CreateConfigMap(cm)
			}
			return err
		}
		if cluster.Status.Restore.Phase == redisv1alpha1.RestorePhaseRestart && restoreCm.Data[configmaps.RestoreSucceeded] == "0" {
			cm := configmaps.NewConfigMapForRestore(cluster, labels)
			return r.configMapClient.UpdateConfigMap(cm)
		}
	}
	return nil
}

func (r *realEnsureResource) EnsureRedisRCloneSecret(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error {
	if !cluster.IsRestoreFromBackup() || cluster.IsRestored() {
		return nil
	}
	backup := cluster.Status.Restore.Backup
	secret, err := osm.NewRcloneSecret(r.client, backup.RCloneSecretName(), cluster.Namespace, backup.Spec.Backend, redisv1alpha1.DefaultOwnerReferences(cluster))
	if err != nil {
		return err
	}
	if err := k8sutil.CreateSecret(r.client, secret, r.logger); err != nil {
		return err
	}
	return nil
}

func isRedisConfChanged(confInCm string, currentConf map[string]string, log logr.Logger) bool {
	lines := strings.Split(strings.TrimSuffix(confInCm, "\n"), "\n")
	if len(lines) != len(currentConf) {
		return true
	}
	for _, line := range lines {
		line = strings.TrimSuffix(line, " ")
		confLine := strings.SplitN(line, " ", 2)
		if len(confLine) == 2 {
			if valueInCurrentConf, ok := currentConf[confLine[0]]; !ok {
				return true
			} else {
				if valueInCurrentConf != confLine[1] {
					return true
				}
			}
		} else {
			log.Info("custom config is invalid", "raw", line, "split", confLine)
		}
	}
	return false
}

func (r *realEnsureResource) UpdateRedisStatefulsets(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) error {
	for i := 0; i < int(cluster.Spec.MasterSize); i++ {
		name := statefulsets.ClusterStatefulSetName(cluster.Name, i)
		svcName := statefulsets.ClusterHeadlessSvcName(cluster.Spec.ServiceName, i)
		// assign label
		labels[redisv1alpha1.StatefulSetLabel] = name
		if err := r.updateRedisStatefulset(cluster, name, svcName, labels); err != nil {
			return err
		}
	}
	return nil
}

func (r *realEnsureResource) updateRedisStatefulset(cluster *redisv1alpha1.DistributedRedisCluster, ssName, svcName string,
	labels map[string]string) error {
	r.logger.WithValues("StatefulSet.Namespace", cluster.Namespace, "StatefulSet.Name", ssName).
		Info("updating statefulSet immediately")
	newSS, err := statefulsets.NewStatefulSetForCR(cluster, ssName, svcName, labels)
	if err != nil {
		return err
	}
	return r.statefulSetClient.UpdateStatefulSet(newSS)
}
