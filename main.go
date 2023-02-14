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
		RegisterBackupItemAction("velero.io/vsm-volumesnapshotcontent-backupper", newVolumeSnapContentBackupItemAction).
		RegisterBackupItemAction("velero.io/vsm-volumesnapshotbackup-backupper", newVolumeSnapshotBackupBackupItemAction).
		RegisterRestoreItemAction("velero.io/vsm-volumesnapshot-restorer", newVolumeSnapshotRestoreItemAction).
		RegisterRestoreItemAction("velero.io/vsm-volumesnapshotclass-restorer", newVolumeSnapshotClassRestoreItemAction).
		RegisterRestoreItemAction("velero.io/vsm-datamover-restorer", newVolumeSnapshotRestoreRestoreItemAction).
		RegisterDeleteItemAction("velero.io/csi-volumesnapshotbackup-delete", newVolumeSnapshotBackupDeleteItemAction).
		Serve()
}

func newVolumeSnapContentBackupItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &backup.VolumeSnapshotContentBackupItemAction{Log: logger}, nil
}

func newVolumeSnapshotBackupBackupItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &backup.VolumeSnapshotBackupBackupItemAction{Log: logger}, nil
}

func newVolumeSnapshotRestoreItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &restore.VolumeSnapshotRestoreItemAction{Log: logger}, nil
}

func newVolumeSnapshotClassRestoreItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &restore.VolumeSnapshotClassRestoreItemAction{Log: logger}, nil
}

func newVolumeSnapshotRestoreRestoreItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &restore.VolumeSnapshotRestoreRestoreItemAction{Log: logger}, nil
}

func newVolumeSnapshotBackupDeleteItemAction(logger logrus.FieldLogger) (interface{}, error) {
	return &delete.VolumeSnapshotBackupDeleteItemAction{Log: logger}, nil
}
