package backup

import (
	"context"
	"fmt"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// Add temp VSClass as additional item
	tempVSC := snapshotv1api.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-snapclass", backup.Name),
			Labels: map[string]string{
				util.WaitVolumeSnapshotBackup: "true",
			},
		},
		// dummy driver as it is not used, but the field is required
		Driver:         "foo",
		DeletionPolicy: "Retain",
	}

	_, snapshotClient, err := util.GetClients()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	_, err = snapshotClient.SnapshotV1().VolumeSnapshotClasses().Create(context.TODO(), &tempVSC, metav1.CreateOptions{})

	if apierrors.IsAlreadyExists(err) {
		p.Log.Infof("skipping creation of temp volumesnapshotclass %v as already exists", tempVSC.Name)

	} else if err != nil {
		return nil, nil, errors.Wrapf(err, "error creating temp volumesnapshotclass")
	}

	additionalItems := []velero.ResourceIdentifier{}

	// adding temp VSClass instance as an additional item for blocking VSRs
	additionalItems = append(additionalItems, velero.ResourceIdentifier{
		GroupResource: kuberesource.VolumeSnapshotClasses,
		Name:          tempVSC.Name,
	})

	return &unstructured.Unstructured{Object: vsbMap}, additionalItems, nil
}
