package statefulsets

import (
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
	"github.com/ucloud/redis-cluster-operator/pkg/config"
	"github.com/ucloud/redis-cluster-operator/pkg/osm"
	"github.com/ucloud/redis-cluster-operator/pkg/resources/configmaps"
	"github.com/ucloud/redis-cluster-operator/pkg/utils"
)

var log = logf.Log.WithName("resource_statefulset")

const (
	redisStorageVolumeName      = "redis-data"
	redisRestoreLocalVolumeName = "redis-local"
	redisServerName             = "redis"
	hostnameTopologyKey         = "kubernetes.io/hostname"
	ExporterContainerName       = "exporter"

	graceTime = 30

	configMapVolumeName = "conf"
)

// NewStatefulSetForCR creates a new StatefulSet for the given Cluster.
// 为给定cluster生成stateFulSet(的定义)
func NewStatefulSetForCR(cluster *redisv1alpha1.DistributedRedisCluster, ssName, svcName string,
	labels map[string]string) (*appsv1.StatefulSet, error) {
	password := redisPassword(cluster) //给定cluster的password定义
	//redisVolumes 确定stateFulSet的volumes, 包含conf(configmap类型)、dataVolume、备份的volume等
	volumes := redisVolumes(cluster) //redisVolumes 确定stateFulSet的volumes, 包含conf(configmap类型)、dataVolume、备份的volume等
	namespace := cluster.Namespace
	spec := cluster.Spec
	size := spec.ClusterReplicas + 1
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ssName,
			Namespace:       namespace,
			Labels:          labels,
			OwnerReferences: redisv1alpha1.DefaultOwnerReferences(cluster),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: svcName,
			Replicas:    &size,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType, // RollingUpdate
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: cluster.Spec.Annotations,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: cluster.Spec.ImagePullSecrets, //如果拉取镜像需要密码,则这里设置
					Affinity:         getAffinity(cluster, labels),
					Tolerations:      spec.ToleRations,
					SecurityContext:  spec.SecurityContext,
					NodeSelector:     cluster.Spec.NodeSelector,
					Containers: []corev1.Container{
						redisServerContainer(cluster, password),
					},
					Volumes: volumes,
				},
			},
		},
	}

	// 如果cluster声明的是pvc,则将pvc信息加入到 stateFulSet.Spec.VolumeClaimTemplates中
	// clustere.spec.Storate.Type == "persistent-claim"
	if spec.Storage != nil && spec.Storage.Type == redisv1alpha1.PersistentClaim {
		ss.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{
			//从cluster的spec.storage中获取 pvc的相关声明(如accessMpde、storaageClassName等)
			persistentClaim(cluster, labels),
		}
		if spec.Storage.DeleteClaim {
			// set an owner reference so the persistent volumes are deleted when the cluster be deleted.
			ss.Spec.VolumeClaimTemplates[0].OwnerReferences = redisv1alpha1.DefaultOwnerReferences(cluster)
		}
	}
	if spec.Monitor != nil {
		//redisExporterContainer 返回redis exporter container的定义
		ss.Spec.Template.Spec.Containers = append(ss.Spec.Template.Spec.Containers, redisExporterContainer(cluster, password))
	}
	//需要从备份中恢复数据
	if cluster.IsRestoreFromBackup() && cluster.IsRestoreRunning() && cluster.Status.Restore.Backup != nil {
		//redisInitContainer 主要作用是在init Container中根据备份恢复数据
		initContainer, err := redisInitContainer(cluster, password)
		if err != nil {
			return nil, err
		}
		ss.Spec.Template.Spec.InitContainers = append(ss.Spec.Template.Spec.InitContainers, initContainer)
	}
	return ss, nil
}

func getAffinity(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) *corev1.Affinity {
	affinity := cluster.Spec.Affinity
	if affinity != nil {
		return affinity
	}

	if cluster.Spec.RequiredAntiAffinity {
		return &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							TopologyKey: hostnameTopologyKey,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{redisv1alpha1.LabelClusterName: cluster.Name},
							},
						},
					},
				},
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						TopologyKey: hostnameTopologyKey,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: labels,
						},
					},
				},
			},
		}
	}
	// return a SOFT anti-affinity by default
	return &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						TopologyKey: hostnameTopologyKey,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{redisv1alpha1.LabelClusterName: cluster.Name},
						},
					},
				},
			},
		},
	}
}

//persistentClaim 从cluster的spec.storage中获取 pvc的相关声明(如accessMpde、storaageClassName等)
func persistentClaim(cluster *redisv1alpha1.DistributedRedisCluster, labels map[string]string) corev1.PersistentVolumeClaim {
	mode := corev1.PersistentVolumeFilesystem
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   redisStorageVolumeName, //redis-data
			Labels: labels,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, // ["ReadWriteOnce"]
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: cluster.Spec.Storage.Size,
				},
			},
			StorageClassName: &cluster.Spec.Storage.Class,
			VolumeMode:       &mode, //Filesystem
		},
	}
}

//ClusterStatefulSetName 集群statefulSet设置名字: drc-${cluster_name}-${N}
func ClusterStatefulSetName(clusterName string, i int) string {
	return fmt.Sprintf("drc-%s-%d", clusterName, i)
}

//ClusterHeadlessSvcName 集群每个stateFulSet都对应一个headlessSvc,名称:${serviceName}-${N}
func ClusterHeadlessSvcName(name string, i int) string {
	return fmt.Sprintf("%s-%d", name, i)
}

func getRedisCommand(cluster *redisv1alpha1.DistributedRedisCluster, password *corev1.EnvVar) []string {
	cmd := []string{
		"/conf/fix-ip.sh",
		"redis-server",
		"/conf/redis.conf",
		"--cluster-enabled yes",
		"--cluster-config-file /data/nodes.conf",
	}
	if password != nil {
		cmd = append(cmd, fmt.Sprintf("--requirepass '$(%s)'", redisv1alpha1.PasswordENV),
			fmt.Sprintf("--masterauth '$(%s)'", redisv1alpha1.PasswordENV))
	}

	renameCmdMap := utils.BuildCommandReplaceMapping(config.RedisConf().GetRenameCommandsFile(), log)
	mergedCmd := mergeRenameCmds(cluster.Spec.Command, renameCmdMap)

	if len(mergedCmd) > 0 {
		cmd = append(cmd, mergedCmd...)
	}

	return cmd
}

func mergeRenameCmds(userCmds []string, systemRenameCmdMap map[string]string) []string {
	cmds := make([]string, 0)
	for _, cmd := range userCmds {
		splitedCmd := strings.Fields(cmd)
		if len(splitedCmd) == 3 && strings.ToLower(splitedCmd[0]) == "--rename-command" {
			if _, ok := systemRenameCmdMap[splitedCmd[1]]; !ok {
				cmds = append(cmds, cmd)
			}
		} else {
			cmds = append(cmds, cmd)
		}
	}

	renameCmdSlice := make([]string, len(systemRenameCmdMap))
	i := 0
	for key, value := range systemRenameCmdMap {
		c := fmt.Sprintf("--rename-command %s %s", key, value)
		renameCmdSlice[i] = c
		i++
	}
	sort.Strings(renameCmdSlice)
	for _, renameCmd := range renameCmdSlice {
		cmds = append(cmds, renameCmd)
	}

	return cmds
}

//redisServerContainer 生成redis server container的定义
func redisServerContainer(cluster *redisv1alpha1.DistributedRedisCluster, password *corev1.EnvVar) corev1.Container {
	probeArg := "redis-cli -h $(hostname) ping"

	container := corev1.Container{
		Name:            redisServerName,
		Image:           cluster.Spec.Image,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,
		SecurityContext: cluster.Spec.ContainerSecurityContext,
		Ports: []corev1.ContainerPort{
			{
				Name:          "client",
				ContainerPort: 6379,
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "gossip",
				ContainerPort: 16379,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		VolumeMounts: volumeMounts(),
		Command:      getRedisCommand(cluster, password),
		LivenessProbe: &corev1.Probe{
			InitialDelaySeconds: graceTime,
			TimeoutSeconds:      5,
			Handler: corev1.Handler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"sh",
						"-c",
						probeArg,
					},
				},
			},
		},
		ReadinessProbe: &corev1.Probe{
			InitialDelaySeconds: graceTime,
			TimeoutSeconds:      5,
			Handler: corev1.Handler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"sh",
						"-c",
						probeArg,
					},
				},
			},
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
		},
		Resources: *cluster.Spec.Resources,
		// TODO store redis data when pod stop
		Lifecycle: &corev1.Lifecycle{
			PostStart: &corev1.Handler{
				Exec: &corev1.ExecAction{
					Command: []string{"/bin/sh", "-c", "echo ${REDIS_PASSWORD} > /data/redis_password"},
				},
			},
			PreStop: &corev1.Handler{
				Exec: &corev1.ExecAction{
					Command: []string{"/bin/sh", "/conf/shutdown.sh"},
				},
			},
		},
	}

	if password != nil {
		container.Env = append(container.Env, *password)
	}

	container.Env = customContainerEnv(container.Env, cluster.Spec.Env)

	return container
}

//redisExporterContainer 返回redis exporter container的定义
func redisExporterContainer(cluster *redisv1alpha1.DistributedRedisCluster, password *corev1.EnvVar) corev1.Container {
	container := corev1.Container{
		Name: ExporterContainerName,
		Args: append([]string{
			fmt.Sprintf("--web.listen-address=:%v", cluster.Spec.Monitor.Prometheus.Port),
			fmt.Sprintf("--web.telemetry-path=%v", redisv1alpha1.PrometheusExporterTelemetryPath),
		}, cluster.Spec.Monitor.Args...),
		Image:           cluster.Spec.Monitor.Image,
		ImagePullPolicy: corev1.PullAlways,
		Ports: []corev1.ContainerPort{
			{
				Name:          "prom-http",
				Protocol:      corev1.ProtocolTCP,
				ContainerPort: cluster.Spec.Monitor.Prometheus.Port,
			},
		},
		Env:             cluster.Spec.Monitor.Env,
		Resources:       cluster.Spec.Monitor.Resources,
		SecurityContext: cluster.Spec.Monitor.SecurityContext,
	}
	if password != nil {
		container.Env = append(container.Env, *password)
	}

	container.Env = customContainerEnv(container.Env, cluster.Spec.Env)

	return container
}

//redisInitContainer init Container,主要作用是在init Container中根据备份恢复数据
func redisInitContainer(cluster *redisv1alpha1.DistributedRedisCluster, password *corev1.EnvVar) (corev1.Container, error) {
	backup := cluster.Status.Restore.Backup
	backupSpec := backup.Spec.Backend
	location, err := backupSpec.Location()
	if err != nil {
		return corev1.Container{}, err
	}
	folderName, err := backup.RemotePath()
	if err != nil {
		return corev1.Container{}, err
	}
	log.V(3).Info("restore", "namespaces", cluster.Namespace, "name", cluster.Name, "folderName", folderName)
	container := corev1.Container{
		Name:            redisv1alpha1.JobTypeRestore,
		Image:           backup.Spec.Image,
		ImagePullPolicy: corev1.PullAlways,
		Args: []string{
			redisv1alpha1.JobTypeRestore,
			fmt.Sprintf(`--data-dir=%s`, redisv1alpha1.BackupDumpDir),
			fmt.Sprintf(`--location=%s`, location),
			fmt.Sprintf(`--folder=%s`, folderName),
			fmt.Sprintf(`--snapshot=%s`, backup.Name),
			"--",
		},
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name: "REDIS_RESTORE_SUCCEEDED",
				ValueFrom: &corev1.EnvVarSource{
					ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: configmaps.RestoreConfigMapName(cluster.Name),
						},
						Key: configmaps.RestoreSucceeded,
					},
				},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      redisStorageVolumeName,
				MountPath: redisv1alpha1.BackupDumpDir,
			},
			{
				Name:      "rcloneconfig",
				ReadOnly:  true,
				MountPath: osm.SecretMountPath,
			},
		},
	}

	if backup.IsRefLocalPVC() {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      redisRestoreLocalVolumeName,
			MountPath: backup.Spec.Backend.Local.MountPath,
			SubPath:   backup.Spec.Backend.Local.SubPath,
			ReadOnly:  true,
		})
	}

	if password != nil {
		container.Env = append(container.Env, *password)
	}

	if backup.Spec.PodSpec != nil {
		container.Resources = backup.Spec.PodSpec.Resources
		container.LivenessProbe = backup.Spec.PodSpec.LivenessProbe
		container.ReadinessProbe = backup.Spec.PodSpec.ReadinessProbe
		container.Lifecycle = backup.Spec.PodSpec.Lifecycle
	}

	container.Env = customContainerEnv(container.Env, cluster.Spec.Env)

	return container, nil
}

func customContainerEnv(env []corev1.EnvVar, customEnv []corev1.EnvVar) []corev1.EnvVar {
	env = append(env, customEnv...)
	return env
}

func volumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      redisStorageVolumeName,
			MountPath: "/data",
		},
		{
			Name:      configMapVolumeName,
			MountPath: "/conf",
		},
	}
}

// Returns the REDIS_PASSWORD environment variable.
func redisPassword(cluster *redisv1alpha1.DistributedRedisCluster) *corev1.EnvVar {
	if cluster.Spec.PasswordSecret == nil {
		return nil
	}
	secretName := cluster.Spec.PasswordSecret.Name

	return &corev1.EnvVar{
		Name: redisv1alpha1.PasswordENV,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secretName,
				},
				Key: "password",
			},
		},
	}
}

//redisVolumes 确定stateFulSet的volumes, 包含conf(configmap类型)、dataVolume、备份的volume等
func redisVolumes(cluster *redisv1alpha1.DistributedRedisCluster) []corev1.Volume {
	executeMode := int32(0755)
	volumes := []corev1.Volume{
		{
			Name: configMapVolumeName, // conf, 保存fix-ip.sh、redis.conf、shutdown.sh等
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configmaps.RedisConfigMapName(cluster.Name), // 集群conifgmap名就是 redis-cluster-${cluster_name}
					},
					DefaultMode: &executeMode,
				},
			},
		},
	}

	//redisDataVolume 该函数会去找到 user 期望的volume
	//- 如果没有定义 cluster.Spec,则返回 emptyVolume;
	//- 如果 cluster.spec.storate.Type== ephemeral,则返回 emptyVolume;
	//- 如果 cluster.spec.storate.Type== persistent-claim,则返回 nil;
	//- 否则都返回 emptyVolume
	dataVolume := redisDataVolume(cluster)
	if dataVolume != nil {
		volumes = append(volumes, *dataVolume)
	}

	//如果不需要在备份中恢复数据、也没用正在从备份中恢复数据 则直接返回volumes
	if !cluster.IsRestoreFromBackup() ||
		cluster.Status.Restore.Backup == nil ||
		!cluster.IsRestoreRunning() {
		return volumes
	}

	//需要从备份中恢复数据
	volumes = append(volumes, corev1.Volume{
		Name: "rcloneconfig",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: cluster.Status.Restore.Backup.RCloneSecretName(), // rcloneconfig-{backupName}
			},
		},
	})

	//IsRefLocalPVC (备份)是否在localPVC中, backup.Spec.Local != nil && backup.Spec.Local.PersistentVolumeClaim!=nil
	if cluster.Status.Restore.Backup.IsRefLocalPVC() {
		volumes = append(volumes, corev1.Volume{
			Name:         redisRestoreLocalVolumeName,
			VolumeSource: cluster.Status.Restore.Backup.Spec.Local.VolumeSource,
		})
	}

	return volumes
}

func emptyVolume() *corev1.Volume {
	return &corev1.Volume{
		Name: redisStorageVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}

//redisDataVolume 该函数会去找到 user 期望的volume
//- 如果没有定义 cluster.Spec,则返回 emptyVolume;
//- 如果 cluster.spec.storate.Type== ephemeral,则返回 emptyVolume;
//- 如果 cluster.spec.storate.Type== persistent-claim,则返回 nil;
//- 否则都返回 emptyVolume
func redisDataVolume(cluster *redisv1alpha1.DistributedRedisCluster) *corev1.Volume {
	// This will find the volumed desired by the user. If no volume defined
	// an EmptyDir will be used by default
	if cluster.Spec.Storage == nil {
		return emptyVolume()
	}

	switch cluster.Spec.Storage.Type {
	case redisv1alpha1.Ephemeral:
		return emptyVolume()
	case redisv1alpha1.PersistentClaim:
		return nil
	default:
		return emptyVolume()
	}
}
