/*
Copyright 2019, 2020 the Velero contributors.

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

package main

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/backup"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/delete"
	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/restore"
	veleroplugin "github.com/vmware-tanzu/velero/pkg/plugin/framework"
)

func main() {
	veleroplugin.NewServer().
		BindFlags(pflag.CommandLine).
		RegisterBackupItemActionV2("velero.io/vsm-volumesnapshotcontent-backupper", newVolumeSnapContentBackupItemActionV2).
		RegisterBackupItemAction("velero.io/vsm-volumesnapshotbackup-backupper", newVolumeSnapshotBackupBackupItemAction).
		RegisterRestoreItemAction("velero.io/vsm-volumesnapshot-restorer", newVolumeSnapshotRestoreItemAction).
		RegisterRestoreItemActionV2("velero.io/vsm-datamover-restorer", newVolumeSnapshotRestoreRestoreItemActionV2).
		RegisterDeleteItemAction("velero.io/csi-volumesnapshotbackup-delete", newVolumeSnapshotBackupDeleteItemAction).
		Serve()
}

func newVolumeSnapContentBackupItemActionV2(logger logrus.FieldLogger) (interface{}, error) {
	return &backup.VolumeSnapshotContentBackupItemActionV2{Log: logger}, nil
}

func newVolumeSnapshotBackupBackupItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &backup.VolumeSnapshotBackupBackupItemAction{Log: logger}, nil
}

func newVolumeSnapshotRestoreItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &restore.VolumeSnapshotRestoreItemAction{Log: logger}, nil
}

func newVolumeSnapshotRestoreRestoreItemActionV2(logger logrus.FieldLogger) (interface{}, error) {
	return &restore.VolumeSnapshotRestoreRestoreItemActionV2{Log: logger}, nil
}

func newVolumeSnapshotBackupDeleteItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &delete.VolumeSnapshotBackupDeleteItemAction{Log: logger}, nil
}
