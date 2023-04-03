package restore

import (
	"context"
	"strings"
	"time"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// VolumeSnapshotBackupRestoreItemActionV2 is a restore item action plugin to retrieve
// VolumeSnapshotBackup from backup and create VolumeSnapshotRestore
type VolumeSnapshotBackupRestoreItemActionV2 struct {
	Log logrus.FieldLogger
}

func (p *VolumeSnapshotBackupRestoreItemActionV2) Name() string {
	return "VolumeSnapshotBackupRestoreItemActionV2"
}

// AppliesTo returns information indicating that the VolumeSnapshotBackupRestoreItemAction should be invoked
func (p *VolumeSnapshotBackupRestoreItemActionV2) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Info("VolumeSnapshotBackupRestoreItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotbackups.datamover.oadp.openshift.io"},
	}, nil
}

// Execute backs up a VolumeSnapshotBackup object with a completely filled status
func (p *VolumeSnapshotBackupRestoreItemActionV2) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {

	p.Log.Infof("Executing VolumeSnapshotBackupRestoreItemActionV2")
	p.Log.Infof("Executing on item: %v", input.Item)
	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}

	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &vsb); err != nil {
		return &velero.RestoreItemActionExecuteOutput{}, errors.Wrapf(err, "failed to convert VSB input.Item from unstructured")
	}

	operationID := ""

	// create VSR per VSB
	vsr := datamoverv1alpha1.VolumeSnapshotRestore{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "vsr-",
			Namespace:    vsb.Namespace,
			Labels: map[string]string{
				util.RestoreNameLabel:           input.Restore.Name,
				util.PersistentVolumeClaimLabel: vsb.Annotations[util.VolumeSnapshotMoverSourcePVCName],
			},
		},
		Spec: datamoverv1alpha1.VolumeSnapshotRestoreSpec{
			ResticSecretRef: corev1.LocalObjectReference{
				Name: vsb.Spec.ResticSecretRef.Name,
			},
			VolumeSnapshotMoverBackupref: datamoverv1alpha1.VSBRef{
				BackedUpPVCData: datamoverv1alpha1.PVCData{
					Name:             vsb.Annotations[util.VolumeSnapshotMoverSourcePVCName],
					Size:             vsb.Annotations[util.VolumeSnapshotMoverSourcePVCSize],
					StorageClassName: vsb.Annotations[util.VolumeSnapshotMoverSourcePVCStorageClass],
				},
				ResticRepository:        vsb.Annotations[util.VolumeSnapshotMoverResticRepository],
				VolumeSnapshotClassName: vsb.Annotations[util.VolumeSnapshotMoverVolumeSnapshotClass],
			},
			ProtectedNamespace: vsb.Spec.ProtectedNamespace,
		},
	}

	vsrClient, err := util.GetVolumeSnapshotMoverClient()
	if err != nil {
		return nil, err
	}

	// if namespace mapping is specified
	if val, ok := input.Restore.Spec.NamespaceMapping[vsr.GetNamespace()]; ok {
		vsr.SetNamespace(val)
	}

	err = vsrClient.Create(context.Background(), &vsr)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating volumesnapshotrestore CR")
	}
	p.Log.Infof("[vsb-restore] vsr created: %s", vsr.Name)

	// fetch the VSR so we get the name of the VSR as we use generate name for VSR CR creation
	err = vsrClient.Get(context.Background(), client.ObjectKey{Namespace: vsr.Namespace, Name: vsr.Name}, &vsr)
	if err != nil {
		return nil, errors.Wrapf(err, "error fetching volumesnapshotrestore CR for suppyling operationID")
	}

	// operationID for our datamover usecase is VSR NamespacedName which will unique per operation
	operationID = vsr.Namespace + "/" + vsr.Name

	p.Log.Info("Returning from VolumeSnapshotBackupRestoreItemActionV2")

	// don't restore VSB
	return &velero.RestoreItemActionExecuteOutput{
		SkipRestore: true, OperationID: operationID,
	}, nil
}

func (p *VolumeSnapshotBackupRestoreItemActionV2) Progress(operationID string, restore *v1.Restore) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}

	// handle empty operationID case
	if operationID == "" {
		return progress, riav2.InvalidOperationIDError(operationID)
	}

	// fetch the VSR matching the operationID supplied, read its status and return progress of datamovement
	vsrClient, err := util.GetVolumeSnapshotMoverClient()
	vsr := datamoverv1alpha1.VolumeSnapshotRestore{}
	if err != nil {
		return progress, errors.Wrapf(err, "error getting volumesnapshotrestore client")
	}

	splitOperationID := strings.Split(operationID, "/")
	if len(splitOperationID) != 2 {
		return progress, riav2.InvalidOperationIDError(operationID)
	}

	VSRNamespace := splitOperationID[0]
	VSRName := splitOperationID[1]

	err = vsrClient.Get(context.Background(), client.ObjectKey{Namespace: VSRNamespace, Name: VSRName}, &vsr)
	if err != nil {
		return progress, errors.Wrapf(err, "error fetching volumesnapshotrestore CR for operationID: %s", operationID)
	}

	// update progress status via VSR phases
	if vsr.Status.Phase != "" && vsr.Status.BatchingStatus != "" {

		progressDescriptionPhase := string(vsr.Status.Phase)
		progressDescriptionBatchingStatus := string(vsr.Status.BatchingStatus)
		progress.Description = "Phase: " + progressDescriptionPhase + " BatchingStatus: " + progressDescriptionBatchingStatus

		p.Log.Infof("current progress description is: %s", progress.Description)

		if vsr.Status.Phase == datamoverv1alpha1.SnapMoverRestorePhaseCompleted {
			progress.Completed = true
		}

		if vsr.Status.Phase == datamoverv1alpha1.SnapMoverRestorePhaseFailed {
			progress.Err = "VolumeSnapshotRestore has a failed status"
			progress.Completed = true
		}
	}

	// update progress timestamps
	if vsr.Status.StartTimestamp != nil {
		progress.Started = vsr.Status.StartTimestamp.Time
	}

	// mark updated timestamp
	progress.Updated = time.Now()

	return progress, nil
}

// empty func to satisfy riav2 interface
func (p *VolumeSnapshotBackupRestoreItemActionV2) Cancel(operationID string, restore *v1.Restore) error {
	return nil
}

// empty func to satisfy riav2 interface
func (p *VolumeSnapshotBackupRestoreItemActionV2) AreAdditionalItemsReady(additionalItems []velero.ResourceIdentifier, restore *v1.Restore) (bool, error) {
	return true, nil
}
