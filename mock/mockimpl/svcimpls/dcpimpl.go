package svcimpls

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockdb"
)

type dcpImpl struct {
}

func (dcp *dcpImpl) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdDcpOpenConnection, dcp.handleOpenDCPConnection)
	h.RegisterKvHandler(memd.CmdDcpStreamReq, dcp.handleStreamRequest)
	h.RegisterKvHandler(memd.CmdDcpControl, dcp.handleDCPControl)
}

func (dcp *dcpImpl) handleOpenDCPConnection(source mock.KvClient, pak *memd.Packet, start time.Time) {
	fmt.Println("DCP Open Connection")
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpOpenConnection,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

func (dcp *dcpImpl) handleStreamRequest(source mock.KvClient, pak *memd.Packet, start time.Time) {
	fmt.Println("DCP Stream Request")
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpStreamReq,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)

	docs, _, _ := getDocumentFromVBucket(source.SelectedBucket(), uint(pak.Vbucket))

	flags := binary.BigEndian.Uint32(pak.Extras[0:])
	startSeqNo := binary.BigEndian.Uint64(pak.Extras[8:])
	endSeqNo := binary.BigEndian.Uint64(pak.Extras[16:])
	vbUUID := binary.BigEndian.Uint64(pak.Extras[24:])
	snapshotStartSeqNo := binary.BigEndian.Uint64(pak.Extras[32:])
	snapshotEndSeqNo := binary.BigEndian.Uint64(pak.Extras[40:])

	_ = flags
	_ = startSeqNo
	_ = vbUUID
	_ = snapshotStartSeqNo
	_ = snapshotEndSeqNo

	sendSnapshotMarker(source, start, pak.Vbucket, pak.Opaque, 0, uint32(endSeqNo))
	for _, doc := range docs {
		if doc.SeqNo > endSeqNo {
			break
		}
		sendMutation(source, start, pak.Opaque, doc)
		fmt.Println(string(doc.Key))
	}
	sendEndStream(source, start, pak.Vbucket, pak.Opaque)
}

func getDocumentFromVBucket(bucket mock.Bucket, vbIdx uint) ([]*mockdb.Document, uint16, error) {
	vBucket := bucket.Store().GetVbucket(vbIdx)
	vbDocs, err := vBucket.GetAll(0, 0)
	if err != nil {
		return nil, 0, err
	}

	highSeqNo := vBucket.GetHighSeqNo()

	return vbDocs, uint16(highSeqNo), nil
}

func (dcp *dcpImpl) handleDCPControl(source mock.KvClient, pak *memd.Packet, start time.Time) {
	fmt.Println("DCP Control Request")
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: memd.CmdDcpControl,
		Opaque:  pak.Opaque,
		Status:  memd.StatusSuccess,
	}, start)
}

func sendSnapshotMarker(source mock.KvClient, start time.Time, vbucket uint16, opaque, startSeqNo, endSeqNo uint32) {
	extrasBuf := make([]byte, 20)
	binary.BigEndian.PutUint32(extrasBuf[0:], 0)  // Start seqno
	binary.BigEndian.PutUint32(extrasBuf[8:], 0)  // End seqno
	binary.BigEndian.PutUint32(extrasBuf[16:], 1) // Snapshot type

	// Snapshot Marker
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdDcpSnapshotMarker,
		Vbucket:  vbucket,
		Datatype: 0,
		Extras:   extrasBuf,
		Status:   memd.StatusSuccess,
		Opaque:   opaque,
	}, start)
}

func sendMutation(source mock.KvClient, start time.Time, opaque uint32, doc *mockdb.Document) {
	mutationExtrasBuf := make([]byte, 28)
	binary.BigEndian.PutUint32(mutationExtrasBuf[0:], 0)  // by_seqno
	binary.BigEndian.PutUint32(mutationExtrasBuf[8:], 0)  // rev seqno
	binary.BigEndian.PutUint32(mutationExtrasBuf[16:], 0) // flags
	binary.BigEndian.PutUint32(mutationExtrasBuf[20:], 0) // expiration
	binary.BigEndian.PutUint32(mutationExtrasBuf[24:], 0) // lock time
	// Metadata?

	dataType := doc.Datatype
	var value []byte

	if len(doc.Xattrs) > 0 {
		var xattrValues []byte
		for xattrK, xattrV := range doc.Xattrs {
			xattrChunk := []byte(xattrK)
			xattrChunk = append(xattrChunk, byte(0))
			xattrChunk = append(xattrChunk, xattrV...)
			xattrChunk = append(xattrChunk, byte(0))

			xattrChunkLen := make([]byte, 4)
			binary.BigEndian.PutUint32(xattrChunkLen[0:], uint32(len(xattrChunk)))
			xattrChunk = append(xattrChunkLen, xattrChunk...)

			xattrValues = append(xattrValues, xattrChunk...)
		}

		xattrsLen := len(xattrValues)

		value = make([]byte, 4)
		binary.BigEndian.PutUint32(value[0:], uint32(xattrsLen))
		value = append(value, xattrValues...)
		value = append(value, doc.Value...)

		dataType = dataType | uint8(memd.DatatypeFlagXattrs)
	} else {
		value = doc.Value
	}

	// Send mutation
	writePacketToSource(source, &memd.Packet{
		Magic:    memd.CmdMagicReq,
		Command:  memd.CmdDcpMutation,
		Datatype: dataType,
		Vbucket:  uint16(doc.VbID),
		Key:      doc.Key,
		Value:    value,
		Extras:   mutationExtrasBuf,
		Status:   memd.StatusSuccess,
		Opaque:   opaque,
	}, start)

}

func sendEndStream(source mock.KvClient, start time.Time, vbucket uint16, opaque uint32) {
	streamEndExtrasBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(streamEndExtrasBuf[0:], 0) // Flags 0 == OK

	// Stream End
	writePacketToSource(source, &memd.Packet{
		Magic:   memd.CmdMagicReq,
		Command: memd.CmdDcpStreamEnd,
		Vbucket: vbucket,
		Status:  memd.StatusSuccess,
		Opaque:  opaque,
		Extras:  streamEndExtrasBuf,
	}, start)
}
