package types

import "encoding/binary"

type Partition struct {
	ErrorCode              uint16
	PartitionIndex         uint32
	LeaderID               uint32
	LeaderEpoch            uint32
	ReplicaNodes           CompactArray[Replica]
	IsrNodes               CompactArray[Replica]
	EligibleLeaderReplicas CompactArray[Replica]
	LastKnownELRs          CompactArray[Replica]
	OfflineReplicas        CompactArray[Replica]
	TagBuffer              byte
}

func partitionElemWriter(r *Partition) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, r.ErrorCode)
	buf = binary.BigEndian.AppendUint32(buf, r.PartitionIndex)
	buf = binary.BigEndian.AppendUint32(buf, r.LeaderID)
	buf = binary.BigEndian.AppendUint32(buf, r.LeaderEpoch)
	buf = append(buf, r.ReplicaNodes.ToBytes(ReplicaElemWriter)...)
	buf = append(buf, r.IsrNodes.ToBytes(ReplicaElemWriter)...)
	buf = append(buf, r.EligibleLeaderReplicas.ToBytes(ReplicaElemWriter)...)
	buf = append(buf, r.LastKnownELRs.ToBytes(ReplicaElemWriter)...)
	buf = append(buf, r.OfflineReplicas.ToBytes(ReplicaElemWriter)...)
	buf = append(buf, r.TagBuffer)
	return buf
}
