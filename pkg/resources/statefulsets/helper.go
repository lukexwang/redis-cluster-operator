package statefulsets

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1alpha1 "github.com/ucloud/redis-cluster-operator/pkg/apis/redis/v1alpha1"
)

const passwordKey = "password"

// IsPasswordChanged determine whether the password is changed.
// 判断集群的密码是否发生改变,对比statefulSet Container.env中的secret.name 和 cluster.spec.passwordSecret.name
//env:
//- name: POD_IP
//  valueFrom:
//    fieldRef:
//      apiVersion: v1
//      fieldPath: status.podIP
//- name: REDIS_PASSWORD
//  valueFrom:
//    secretKeyRef:
//      key: password
//      name: mysecret (stateFulSet这一部分和 spec.passwordSecret不同,则密码发生了变化)
func IsPasswordChanged(cluster *redisv1alpha1.DistributedRedisCluster, sts *appsv1.StatefulSet) bool {
	if cluster.Spec.PasswordSecret != nil {
		envSet := sts.Spec.Template.Spec.Containers[0].Env
		secretName := getSecretKeyRefByKey(redisv1alpha1.PasswordENV, envSet)
		if secretName == "" {
			return true
		}
		if secretName != cluster.Spec.PasswordSecret.Name {
			return true
		}
	}
	return false
}

func getSecretKeyRefByKey(key string, envSet []corev1.EnvVar) string {
	for _, value := range envSet {
		if key == value.Name {
			if value.ValueFrom != nil && value.ValueFrom.SecretKeyRef != nil {
				return value.ValueFrom.SecretKeyRef.Name
			}
		}
	}
	return ""
}

// GetOldRedisClusterPassword return old redis cluster's password.
// - 先读取cluster中statefFulSet对应的secret name;
// - 再读取secret name内容,从中获取'password'对应的value
func GetOldRedisClusterPassword(client client.Client, sts *appsv1.StatefulSet) (string, error) {
	envSet := sts.Spec.Template.Spec.Containers[0].Env
	secretName := getSecretKeyRefByKey(redisv1alpha1.PasswordENV, envSet)
	if secretName == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Name:      secretName,
		Namespace: sts.Namespace,
	}, secret)
	if err != nil {
		return "", err
	}
	return string(secret.Data[passwordKey]), nil
}

// GetClusterPassword return current redis cluster's password.
// - 先读取cluster.Spec.PasswordSecret对应的secret name;
// - 再读取secret name内容,从中获取'password'对应的value
func GetClusterPassword(client client.Client, cluster *redisv1alpha1.DistributedRedisCluster) (string, error) {
	if cluster.Spec.PasswordSecret == nil {
		return "", nil
	}
	secret := &corev1.Secret{}
	err := client.Get(context.TODO(), types.NamespacedName{
		Name:      cluster.Spec.PasswordSecret.Name,
		Namespace: cluster.Namespace,
	}, secret)
	if err != nil {
		return "", err
	}
	return string(secret.Data[passwordKey]), nil
}
