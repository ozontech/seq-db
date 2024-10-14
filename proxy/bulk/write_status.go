package bulk

type storesWriteStatus struct {
	replicasCnt int
	statuses    []bool
}

func newStoresWriteStatus(shardsCnt, replicasCnt int) *storesWriteStatus {
	return &storesWriteStatus{
		replicasCnt: replicasCnt,
		statuses:    make([]bool, shardsCnt*replicasCnt),
	}
}

func (s *storesWriteStatus) getShard(idx int) []bool {
	shift := idx * s.replicasCnt
	return s.statuses[shift : shift+s.replicasCnt]
}

type bulkWriteStatus struct {
	coldWritten   bool
	hotStoresWS   *storesWriteStatus
	writeStoresWS *storesWriteStatus
}

func newBulkWriteStatus(hotShardsCnt, hotReplicasCnt, writeShardsCnt, writeReplicasCnt int) *bulkWriteStatus {
	return &bulkWriteStatus{
		coldWritten:   false,
		hotStoresWS:   newStoresWriteStatus(hotShardsCnt, hotReplicasCnt),
		writeStoresWS: newStoresWriteStatus(writeShardsCnt, writeReplicasCnt),
	}
}
