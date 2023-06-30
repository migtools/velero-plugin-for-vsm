/*
Copyright 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"context"
	"fmt"
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/label"
)

const (
	//TODO: use annotation from velero https://github.com/vmware-tanzu/velero/pull/2283
	resticPodAnnotation   = "backup.velero.io/backup-volumes"
	ReconciledReasonError = "Error"
	ConditionReconciled   = "Reconciled"

	// Timeout consts
	DefaultVSRTimeout = "10m"
)

func GetPVForPVC(pvc *corev1api.PersistentVolumeClaim, corev1 corev1client.PersistentVolumesGetter) (*corev1api.PersistentVolume, error) {
	if pvc.Spec.VolumeName == "" {
		return nil, errors.Errorf("PVC %s/%s has no volume backing this claim", pvc.Namespace, pvc.Name)
	}
	if pvc.Status.Phase != corev1api.ClaimBound {
		// TODO: confirm if this PVC should be snapshotted if it has no PV bound
		return nil, errors.Errorf("PVC %s/%s is in phase %v and is not bound to a volume", pvc.Namespace, pvc.Name, pvc.Status.Phase)
	}
	pvName := pvc.Spec.VolumeName
	pv, err := corev1.PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get PV %s for PVC %s/%s", pvName, pvc.Namespace, pvc.Name)
	}
	return pv, nil
}

func GetPodsUsingPVC(pvcNamespace, pvcName string, corev1 corev1client.PodsGetter) ([]corev1api.Pod, error) {
	podsUsingPVC := []corev1api.Pod{}
	podList, err := corev1.Pods(pvcNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, p := range podList.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				podsUsingPVC = append(podsUsingPVC, p)
			}
		}
	}

	return podsUsingPVC, nil
}

func GetPodVolumeNameForPVC(pod corev1api.Pod, pvcName string) (string, error) {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
			return v.Name, nil
		}
	}
	return "", errors.Errorf("Pod %s/%s does not use PVC %s/%s", pod.Namespace, pod.Name, pod.Namespace, pvcName)
}

func Contains(slice []string, key string) bool {
	for _, i := range slice {
		if i == key {
			return true
		}
	}
	return false
}

// GetVolumeSnapshotClassForStorageClass returns a VolumeSnapshotClass for the supplied volume provisioner/ driver name.
func GetVolumeSnapshotClassForStorageClass(provisioner string, snapshotClient snapshotter.SnapshotV1Interface) (*snapshotv1api.VolumeSnapshotClass, error) {
	snapshotClasses, err := snapshotClient.VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error listing volumesnapshot classes")
	}
	// We pick the volumesnapshotclass that matches the CSI driver name and has a 'velero.io/csi-volumesnapshot-class'
	// label. This allows multiple VolumesnapshotClasses for the same driver with different values for the
	// other fields in the spec.
	// https://github.com/kubernetes-csi/external-snapshotter/blob/release-4.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
	for _, sc := range snapshotClasses.Items {
		_, hasLabelSelector := sc.Labels[VolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasLabelSelector {
			return &sc, nil
		}
	}
	return nil, errors.Errorf("failed to get volumesnapshotclass for provisioner %s, ensure that the desired volumesnapshot class has the %s label", provisioner, VolumeSnapshotClassSelectorLabel)
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the volumesnapshotcontent object associated with the volumesnapshot
func GetVolumeSnapshotContentForVolumeSnapshot(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger, shouldWait bool) (*snapshotv1api.VolumeSnapshotContent, error) {
	if !shouldWait {
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *volSnap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "error getting volume snapshot content from API")
		}
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling every 5s
	// TODO: make this timeout configurable.
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	var snapshotContent *snapshotv1api.VolumeSnapshotContent

	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name))
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volSnap.Namespace, volSnap.Name, interval/time.Second)
			return false, nil
		}

		snapshotContent, err = snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
		}

		// we need to wait for the VolumeSnaphotContent to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if snapshotContent.Status == nil || snapshotContent.Status.SnapshotHandle == nil {
			log.Infof("Waiting for volumesnapshotcontents %s to have snapshot handle. Retrying in %ds", snapshotContent.Name, interval/time.Second)
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
		}
		return nil, err
	}

	return snapshotContent, nil
}

func GetClients() (*kubernetes.Clientset, *snapshotterClientSet.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	snapshotterClient, err := snapshotterClientSet.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return client, snapshotterClient, nil
}

// IsVolumeSnapshotClassHasListerSecret returns whether a volumesnapshotclass has a snapshotlister secret
func IsVolumeSnapshotClassHasListerSecret(vc *snapshotv1api.VolumeSnapshotClass) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L59-L60
	// There is no release w/ these constants exported. Using the strings for now.
	_, nameExists := vc.Annotations[PrefixedSnapshotterListSecretNameKey]
	_, nsExists := vc.Annotations[PrefixedSnapshotterListSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotContentHasDeleteSecret returns whether a volumesnapshotcontent has a deletesnapshot secret
func IsVolumeSnapshotContentHasDeleteSecret(vsc *snapshotv1api.VolumeSnapshotContent) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L56-L57
	// use exported constants in the next release
	_, nameExists := vsc.Annotations[PrefixedSnapshotterSecretNameKey]
	_, nsExists := vsc.Annotations[PrefixedSnapshotterSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotHasVSCDeleteSecret returns whether a volumesnapshot should set the deletesnapshot secret
// for the static volumesnapshotcontent that is created on restore
func IsVolumeSnapshotHasVSCDeleteSecret(vs *snapshotv1api.VolumeSnapshot) bool {
	_, nameExists := vs.Annotations[CSIDeleteSnapshotSecretName]
	_, nsExists := vs.Annotations[CSIDeleteSnapshotSecretNamespace]
	return nameExists && nsExists
}

// AddAnnotations adds the supplied key-values to the annotations on the object
func AddAnnotations(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Annotations == nil {
		o.Annotations = make(map[string]string)
	}
	for k, v := range vals {
		o.Annotations[k] = v
	}
}

// AddLabels adds the supplied key-values to the labels on the object
func AddLabels(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Labels == nil {
		o.Labels = make(map[string]string)
	}
	for k, v := range vals {
		o.Labels[k] = label.GetValidName(v)
	}
}

// IsVolumeSnapshotExists returns whether a specific volumesnapshot object exists.
func IsVolumeSnapshotExists(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface) bool {
	exists := false
	if volSnap != nil {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err == nil && vs != nil {
			exists = true
		}
	}

	return exists
}

func SetVolumeSnapshotContentDeletionPolicy(vscName string, csiClient snapshotter.SnapshotV1Interface) error {
	pb := []byte(`{"spec":{"deletionPolicy":"Delete"}}`)
	_, err := csiClient.VolumeSnapshotContents().Patch(context.TODO(), vscName, types.MergePatchType, pb, metav1.PatchOptions{})

	return err
}

func HasBackupLabel(o *metav1.ObjectMeta, backupName string) bool {
	if o.Labels == nil || len(strings.TrimSpace(backupName)) == 0 {
		return false
	}
	return o.Labels[velerov1api.BackupNameLabel] == label.GetValidName(backupName)
}

// Get VolumeSnapshotBackup CR with status data
func GetVolumeSnapshotbackupWithStatusData(volumeSnapshotbackupNS string, volumeSnapshotName string, log logrus.FieldLogger) (datamoverv1alpha1.VolumeSnapshotBackup, error) {

	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}
	// default timeout value is 10
	timeoutValue := "10m"
	// use timeout value if configured
	if len(os.Getenv(DatamoverTimeout)) > 0 {
		timeoutValue = os.Getenv(DatamoverTimeout)
	}

	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return vsb, errors.Wrapf(err, "error parsing the datamover timout")
	}
	interval := 5 * time.Second

	snapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		return vsb, err
	}

	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		err := snapMoverClient.Get(context.TODO(), client.ObjectKey{Namespace: volumeSnapshotbackupNS, Name: volumeSnapshotName}, &vsb)
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotbackup %s/%s", volumeSnapshotbackupNS, volumeSnapshotName))
		}

		if len(vsb.Status.Conditions) == 0 {
			log.Infof("Waiting for volumesnapshotbackup %s to have conditions. Retrying in %ds", vsb.Name, interval/time.Second)
			return false, nil
		}

		// check for status failure first
		for _, condition := range vsb.Status.Conditions {
			if condition.Status == metav1.ConditionFalse && condition.Reason == ReconciledReasonError && condition.Type == ConditionReconciled {
				return false, errors.Errorf("volumesnapshotbackup %v has failed status", vsb.Name)
			}
		}

		if len(vsb.Status.ResticRepository) == 0 || len(vsb.Status.SourcePVCData.Name) == 0 || len(vsb.Status.SourcePVCData.Size) == 0 || len(vsb.Status.SourcePVCData.StorageClassName) == 0 || len(vsb.Status.VolumeSnapshotClassName) == 0 {
			log.Infof("Waiting for volumesnapshotbackup %s/%s to have status data. Retrying in %ds", volumeSnapshotbackupNS, volumeSnapshotName, interval/time.Second)
			return false, nil
		}

		return true, nil

	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshotbackup %s/%s", volumeSnapshotbackupNS, volumeSnapshotName)
		}
		return vsb, err
	}
	log.Infof("Return VSB from GetVolumeSnapshotbackupWithInProgressStatus: %v", vsb)
	return vsb, nil
}

// Get VolumeSnapshotBackup CR with status data
func GetVolumeSnapshotRestoreWithStatusData(restoreName string, PVCName string, log logrus.FieldLogger) (datamoverv1alpha1.VolumeSnapshotRestoreList, error) {

	vsrList := datamoverv1alpha1.VolumeSnapshotRestoreList{}
	// default timeout value is 10
	timeoutValue := DefaultVSRTimeout
	// use timeout value if configured
	if len(os.Getenv(DatamoverTimeout)) > 0 {
		timeoutValue = os.Getenv(DatamoverTimeout)
	}

	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return vsrList, errors.Wrapf(err, "error parsing the datamover timout")
	}
	interval := 5 * time.Second

	err = wait.PollImmediate(interval, timeout, func() (bool, error) {

		snapMoverClient, err := GetVolumeSnapshotMoverClient()
		if err != nil {
			return false, err
		}

		VSRListOptions := client.MatchingLabels(map[string]string{
			velerov1api.RestoreNameLabel: restoreName,
			PersistentVolumeClaimLabel:   PVCName,
		})

		err = snapMoverClient.List(context.TODO(), &vsrList, VSRListOptions)
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotrestoreList for PVC %s", PVCName))
		}

		if len(vsrList.Items) > 0 {
			if vsrList.Items[0].Status.Phase == "Failed" || vsrList.Items[0].Status.Phase == "PartiallyFailed" {
				return false, errors.Errorf("volumesnapshotrestore %v has failed status", vsrList.Items[0].Name)
			}

			if len(vsrList.Items[0].Status.SnapshotHandle) == 0 || len(vsrList.Items[0].Status.Phase) == 0 {
				log.Infof("Waiting for volumesnapshotrestore %s to have status data. Retrying in %ds", vsrList.Items[0].Name, interval/time.Second)
				return false, nil
			}
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshotrestoreList")
		}
		return vsrList, err
	}
	log.Debugf("Return VSR from GetVolumeSnapshotrestoreWithInProgressStatus: %v", vsrList)
	return vsrList, nil
}

// Check if volumesnapshotbackup CR exists for a given volumesnapshotcontent
func VSBExistsForVSC(snapCont *snapshotv1api.VolumeSnapshotContent, log logrus.FieldLogger) (bool, error) {

	snapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		return false, err
	}
	vsbList := datamoverv1alpha1.VolumeSnapshotBackupList{}
	VSBListOptions := client.MatchingLabels(map[string]string{
		VolumeSnapshotBackupVolumeSnapshotContent: snapCont.Name,
	})

	err = snapMoverClient.List(context.TODO(), &vsbList, VSBListOptions)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Infof("found volumesnapshotbackup for the given volumesnapshotcontent")
			return true, nil
		}
		return false, err
	}

	log.Infof("did not find volumesnapshotbackup for the given volumesnapshotcontent %v", snapCont.Name)
	return false, nil
}

// Check if volumesnapshotrestore CR exists for a given volumesnapshotbackup
func VSRExistsForVSB(vsb *datamoverv1alpha1.VolumeSnapshotBackup, log logrus.FieldLogger) (bool, error) {

	snapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		return false, err
	}

	vsrList := datamoverv1alpha1.VolumeSnapshotRestoreList{}
	VSRListOptions := client.MatchingLabels(map[string]string{
		VolumeSnapshotBackupLabel: vsb.Name,
	})

	err = snapMoverClient.List(context.TODO(), &vsrList, VSRListOptions)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			log.Infof("found volumesnapshotrestore for the given volumesnapshotbackup")
			return true, nil
		}
		return false, err
	}

	log.Infof("did not find volumesnapshotrestore for the given volumesnapshotbackup %v", vsb.Name)
	return false, nil
}

//Waits for volumesnapshotcontent to be in ready state
func WaitForVolumeSnapshotContentToBeReady(snapCont snapshotv1api.VolumeSnapshotContent, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger) (bool, error) {
	// We'll wait 10m for the VSC to be reconciled polling every 5s
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		updatedVSC, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), snapCont.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s", updatedVSC.Name))
		}
		if updatedVSC.Status == nil || updatedVSC.Status.SnapshotHandle == nil || *updatedVSC.Status.ReadyToUse != true {
			log.Infof("Waiting for volumesnapshotcontents %s to have snapshot handle and be ready. Retrying in %ds", snapCont.Name, interval/time.Second)
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshotcontent %s", snapCont.Name)
		}
		return false, err
	}
	return true, nil
}

func GetVolumeSnapshotMoverClient() (client.Client, error) {
	client2, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}
	datamoverv1alpha1.AddToScheme(client2.Scheme())

	return client2, err
}

func GetVolsyncClient() (client.Client, error) {
	client2, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}
	volsyncv1alpha1.AddToScheme(client2.Scheme())

	return client2, err
}

// We expect VolumeSnapshotMoverEnv to be set once when container is started.
// When true, we will use the csi data-mover code path.
var dataMoverCase, _ = strconv.ParseBool(os.Getenv(VolumeSnapshotMoverEnv))

// DataMoverCase use getter to avoid changing bool in other packages
func DataMoverCase() bool {
	return dataMoverCase
}

func GetDataMoverCredName(backup *velerov1api.Backup, protectedNS string, log logrus.FieldLogger) (string, error) {

	bslName := backup.Spec.StorageLocation
	resticSecretName := fmt.Sprintf("%v-volsync-restic", bslName)

	secretClient, _, err := GetClients()
	if err != nil {
		return "", errors.WithStack(err)
	}

	// check this secret exists
	if _, err := secretClient.CoreV1().Secrets(protectedNS).Get(context.TODO(), resticSecretName, metav1.GetOptions{}); err != nil {
		return "", errors.WithStack(err)
	}

	return resticSecretName, nil
}

func CheckIfVolumeSnapshotRestoresAreComplete(ctx context.Context, volumesnapshotrestores datamoverv1alpha1.VolumeSnapshotRestoreList, log logrus.FieldLogger) error {
	eg, _ := errgroup.WithContext(ctx)
	timeoutValue := "10m"

	// use timeout value if configured
	if len(os.Getenv(DatamoverTimeout)) > 0 {
		timeoutValue = os.Getenv(DatamoverTimeout)
	}
	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return errors.Wrapf(err, "error parsing datamover timout")
	}
	interval := 5 * time.Second

	volumeSnapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		return err
	}

	for _, vsr := range volumesnapshotrestores.Items {
		volumesnapshotrestore := vsr
		eg.Go(func() error {

			err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				tmpVSR := datamoverv1alpha1.VolumeSnapshotRestore{}
				err := volumeSnapMoverClient.Get(ctx, client.ObjectKey{Namespace: volumesnapshotrestore.Namespace, Name: volumesnapshotrestore.Name}, &tmpVSR)
				if err != nil {
					return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotrestore %s/%s", volumesnapshotrestore.Namespace, volumesnapshotrestore.Name))
				}

				// check for a failed VSR
				if tmpVSR.Status.Phase == datamoverv1alpha1.SnapMoverRestorePhaseFailed {
					return false, errors.Errorf("volumesnapshotrestore %s has failed status", tmpVSR.Name)
				}

				// current VSR in list is still in progress
				if len(tmpVSR.Status.SnapshotHandle) == 0 || len(tmpVSR.Status.Phase) == 0 || tmpVSR.Status.Phase != datamoverv1alpha1.SnapMoverRestorePhaseCompleted {
					log.Infof("Waiting for volumesnapshotrestore to complete %s/%s. Retrying in %ds", volumesnapshotrestore.Namespace, volumesnapshotrestore.Name, interval/time.Second)
					return false, nil
				}

				// current VSR in list has completed
				log.Infof("volumesnapshotrestore %s completed", volumesnapshotrestore.Name)
				return true, nil
			})

			if err == wait.ErrWaitTimeout {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshotrestore %s/%s", volumesnapshotrestore.Namespace, volumesnapshotrestore.Name)
			}
			return err
		})
	}
	return eg.Wait()
}

func WaitForDataMoverRestoreToComplete(restoreName string, log logrus.FieldLogger) error {

	//wait for all the VSRs to be complete
	volumeSnapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	VSRList := datamoverv1alpha1.VolumeSnapshotRestoreList{}
	VSRListOptions := client.MatchingLabels(map[string]string{
		velerov1api.RestoreNameLabel: restoreName,
	})

	err = volumeSnapMoverClient.List(context.TODO(), &VSRList, VSRListOptions)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	//Wait for all VSRs to complete
	if len(VSRList.Items) > 0 {

		err = CheckIfVolumeSnapshotRestoresAreComplete(context.Background(), VSRList, log)
		if err != nil {
			log.Errorf("failed to wait for VolumeSnapshotRestores to be completed: %s", err.Error())
			return err
		}
	}
	return nil
}

func VSCBelongsToBackup(backup *velerov1api.Backup, snapCont *snapshotv1api.VolumeSnapshotContent, log logrus.FieldLogger) bool {

	// compare backup name on label with current backup name
	VSCBackupName := snapCont.Labels[BackupNameLabel]
	currentBackupName := backup.Name

	if VSCBackupName != currentBackupName {
		return false
	}

	return true
}

func VSBBelongsToBackup(backupName string, vsb *datamoverv1alpha1.VolumeSnapshotBackup, log logrus.FieldLogger) bool {

	// compare backup name on label with current backup name
	VSBBackupName := vsb.Labels[BackupNameLabel]
	currentBackupName := backupName

	if VSBBackupName != currentBackupName {
		return false
	}

	return true
}

func WaitForVolumeSnapshotSourceToBeReady(volSnap *snapshotv1api.VolumeSnapshot, log logrus.FieldLogger) error {
	if volSnap == nil {
		return errors.New("nil volumeSnapshot in WaitForVolumeSnapshotSourceToBeReady")
	}

	timeoutValue := "10m"

	// use timeout value if configured
	if len(os.Getenv(DatamoverTimeout)) > 0 {
		timeoutValue = os.Getenv(DatamoverTimeout)
	}
	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil {
		return errors.Wrapf(err, "error parsing datamover timout")
	}
	interval := 5 * time.Second

	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		if volSnap.Spec.Source.PersistentVolumeClaimName == nil {
			log.Infof("Waiting for volumesnapshot %s to have source PVC data. Retrying in %ds", volSnap.Name, interval/time.Second)
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			log.Errorf("Timed out awaiting reconciliation of volumesnapshot %s", volSnap.Name)
		}
		return err
	}
	return nil
}

func DeleteVolumeSnapshotContent(snapContName string, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger) error {

	err := snapshotClient.VolumeSnapshotContents().Delete(context.TODO(), snapContName, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "deleting volumesnapshotcontent %v", snapContName)
	}
	return nil
}

func GetVSRsFromBackup(backupName string, vsbName string) (datamoverv1alpha1.VolumeSnapshotRestoreList, error) {

	vsrList := datamoverv1alpha1.VolumeSnapshotRestoreList{}
	snapMoverClient, err := GetVolumeSnapshotMoverClient()
	if err != nil {
		return vsrList, err
	}

	// get VSR(s) associated with specific backup VSB
	vsrListOptions := client.MatchingLabels(map[string]string{
		velerov1api.BackupNameLabel: backupName,
		VolumeSnapshotBackupLabel:   vsbName,
	})

	err = snapMoverClient.List(context.TODO(), &vsrList, vsrListOptions)
	if err != nil {
		return vsrList, err
	}

	return vsrList, nil
}

func GetReplicationSourcesForVSB(vsbName string) (volsyncv1alpha1.ReplicationSourceList, error) {

	rsList := volsyncv1alpha1.ReplicationSourceList{}
	volsyncClient, err := GetVolsyncClient()
	if err != nil {
		return rsList, err
	}

	// get RS(s) associated with specific VSB
	rsListOptions := client.MatchingLabels(map[string]string{
		VSBLabel: vsbName,
	})

	err = volsyncClient.List(context.TODO(), &rsList, rsListOptions)
	if err != nil {
		return rsList, err
	}

	return rsList, nil
}
