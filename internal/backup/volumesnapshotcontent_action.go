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

package backup

import (
	"context"
	"fmt"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	datamoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
)

// VolumeSnapshotContentBackupItemActionV2 is a backup item action plugin to backup
// CSI VolumeSnapshotcontent objects using Velero
type VolumeSnapshotContentBackupItemActionV2 struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the VolumeSnapshotContentBackupItemActionV2 action should be invoked to backup volumesnapshotcontents.
func (p *VolumeSnapshotContentBackupItemActionV2) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("VolumeSnapshotContentBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshotcontent.snapshot.storage.k8s.io"},
	}, nil
}

// Execute returns the unmodified volumesnapshotcontent object along with the snapshot deletion secret, if any, from its annotation
// as additional items to backup.
func (p *VolumeSnapshotContentBackupItemActionV2) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
	p.Log.Infof("Executing VolumeSnapshotContentBackupItemActionV2")

	var snapCont snapshotv1api.VolumeSnapshotContent
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &snapCont); err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	itemsToUpdate := []velero.ResourceIdentifier{}

	// Create VolumeSnapshotBackup CR per VolumeSnapshotContent and add it as an additional item
	operationID := ""
	if util.DataMoverCase() {

		// check the VSC has the same backup name from label as the current backup
		isVSForCurrentBackup := util.VSBHasVSBackupName(backup, &snapCont, p.Log)

		if !isVSForCurrentBackup {
			p.Log.Warnf("stale volumesnapshot found %s", snapCont.Spec.VolumeSnapshotRef.Name)

			return nil, nil, "", nil, nil
		}

		_, snapshotClient, err := util.GetClients()
		if err != nil {
			return nil, nil, "", nil, errors.WithStack(err)
		}

		// Wait for VSC to be in ready state
		VSCReady, err := util.WaitForVolumeSnapshotContentToBeReady(snapCont, snapshotClient.SnapshotV1(), p.Log)

		if err != nil {
			return nil, nil, "", nil, errors.WithStack(err)
		}

		if !VSCReady {
			p.Log.Infof("volumesnapshotcontent not in ready state, still continuing with the backup")
		}

		// get secret name created by data mover controller
		resticSecretName, err := util.GetDataMoverCredName(backup, backup.Namespace, p.Log)
		if err != nil {
			return nil, nil, "", nil, errors.WithStack(err)
		}

		// craft a VolumeBackupSnapshot object to be created
		vsb := datamoverv1alpha1.VolumeSnapshotBackup{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "vsb-",
				Namespace:    snapCont.Spec.VolumeSnapshotRef.Namespace,
				Labels: map[string]string{
					util.BackupNameLabel: backup.Name,
				},
			},
			Spec: datamoverv1alpha1.VolumeSnapshotBackupSpec{
				VolumeSnapshotContent: corev1api.ObjectReference{
					Name: snapCont.Name,
				},
				ProtectedNamespace: backup.Namespace,
				ResticSecretRef: corev1api.LocalObjectReference{
					Name: resticSecretName,
				},
			},
		}

		// check if VolumeBackupSnapshot CR exists for VSC
		VSBExists, err := util.DoesVolumeSnapshotBackupExistForVSC(&snapCont, p.Log)
		if err != nil {
			return nil, nil, "", nil, errors.WithStack(err)
		}

		// Create VSB only if does not exist for the VSC
		if !VSBExists {
			vsbClient, err := util.GetVolumeSnapshotMoverClient()
			if err != nil {
				return nil, nil, "", nil, errors.Wrapf(err, "error getting volumesnapshotbackup client")
			}

			err = vsbClient.Create(context.Background(), &vsb)

			if err != nil {
				return nil, nil, "", nil, errors.Wrapf(err, "error creating volumesnapshotbackup CR")
			}

			p.Log.Infof("Created volumesnapshotbackup %s", fmt.Sprintf("%s/%s", vsb.Namespace, vsb.Name))

			// Now fetch the VSB so that we get the UID from VSB metadata and return that as operationID to be used for progress monitoring
			err = vsbClient.Get(context.Background(), client.ObjectKey{Namespace: vsb.Namespace, Name: vsb.Name}, &vsb)
			if err != nil {
				return nil, nil, "", nil, errors.Wrapf(err, "error fetching volumesnapshotbackup CR for suppyling operationID")
			}

			// operationID for our datamover usecase is VSB NamespacedName which will unique per operation
			operationID = vsb.Namespace + "/" + vsb.Name

			p.Log.Infof("Got operationID: %s", operationID)
		}

		// adding volumesnapshotbackup instance as an item that needs to be updated in backup's finalizing phase with all its annotations and status
		itemsToUpdate = append(itemsToUpdate, velero.ResourceIdentifier{
			GroupResource: schema.GroupResource{Group: "datamover.oadp.openshift.io", Resource: "volumesnapshotbackups"},
			Name:          vsb.Name,
			Namespace:     vsb.Namespace,
		})
	}

	p.Log.Infof("Returning from VolumeSnapshotContentBackupItemActionV2 with %d itemsToUpdate to backup", len(itemsToUpdate))
	return item, nil, operationID, itemsToUpdate, nil
}

func (p *VolumeSnapshotContentBackupItemActionV2) Progress(operationID string, backup *velerov1api.Backup) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}
	p.Log.Infof("Backup name in progress method is: %s", backup.Name)
	p.Log.Infof("OperationID in progress method is: %s", operationID)

	// handle empty operationID case
	if operationID == "" {
		return progress, biav2.InvalidOperationIDError(operationID)
	}

	// fetch the VSB matching the operationID supplied, read its status and return progress of datamovement
	vsbClient, err := util.GetVolumeSnapshotMoverClient()
	vsb := datamoverv1alpha1.VolumeSnapshotBackup{}
	if err != nil {
		return progress, errors.Wrapf(err, "error getting volumesnapshotbackup client")
	}

	splitOperationID := strings.Split(operationID, "/")
	VSBNamespace := splitOperationID[0]
	VSBName := splitOperationID[1]

	err = vsbClient.Get(context.Background(), client.ObjectKey{Namespace: VSBNamespace, Name: VSBName}, &vsb)
	if err != nil {
		return progress, errors.Wrapf(err, "error fetching volumesnapshotbackup CR for operationID: %s", operationID)
	}

	// update progress status via VSB phases
	if vsb.Status.Phase != "" {
		progress.Description = string(vsb.Status.Phase)
		if vsb.Status.Phase == datamoverv1alpha1.SnapMoverBackupPhaseCompleted {
			progress.Completed = true
		}

		if vsb.Status.Phase == datamoverv1alpha1.SnapMoverBackupPhaseFailed {
			progress.Err = "VolumeSnapshotBackup has a failed status"
		}
	}

	// update progress timestamps
	if vsb.Status.StartTimestamp != nil {
		progress.Started = vsb.Status.StartTimestamp.Time
	}

	// treating progress updated field as completion timestamp
	if vsb.Status.CompletionTimestamp != nil {
		progress.Updated = vsb.Status.CompletionTimestamp.Time
	}

	return progress, nil
}

func (p *VolumeSnapshotContentBackupItemActionV2) Cancel(operationID string, backup *velerov1api.Backup) error {
	return nil
}

func (p *VolumeSnapshotContentBackupItemActionV2) Name() string {
	return "VolumeSnapshotContentBackupItemActionV2"
}
