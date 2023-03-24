package backup

import (
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// VolumeSnapshotBackupBackupItemAction is a backup item action plugin to backup
// VolumeSnapshotBackup objects using Velero
type VolumeSnapshotBackupBackupItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the VolumeSnapshotBackupItemAction should be invoked to backup VolumeSnapshotBackups.
func (p *VolumeSnapshotBackupBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Info("VolumeSnapshotBackupBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotbackups.datamover.oadp.openshift.io"},
	}, nil
}

// Execute backs up a VolumeSnapshotBackup object with a completely filled status
func (p *VolumeSnapshotBackupBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	p.Log.Infof("Executing VolumeSnapshotBackupBackupItemAction")
	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &vsb); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	p.Log.Infof("Converted Item to VSB: %v", vsb)

	vsbNew, err := util.GetVolumeSnapshotbackupWithStatusData(vsb.Namespace, vsb.Name, p.Log)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	vsb.Status = *vsbNew.Status.DeepCopy()

	vals := map[string]string{
		util.VolumeSnapshotMoverResticRepository:      vsb.Status.ResticRepository,
		util.VolumeSnapshotMoverSourcePVCName:         vsb.Status.SourcePVCData.Name,
		util.VolumeSnapshotMoverSourcePVCSize:         vsb.Status.SourcePVCData.Size,
		util.VolumeSnapshotMoverSourcePVCStorageClass: vsb.Status.SourcePVCData.StorageClassName,
		util.VolumeSnapshotMoverVolumeSnapshotClass:   vsb.Status.VolumeSnapshotClassName,
	}

	//Add all the relevant status info as annotations because velero strips status subresource for CRDs
	util.AddAnnotations(&vsb.ObjectMeta, vals)

	vsbMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&vsb)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: vsbMap}, nil, nil
}
