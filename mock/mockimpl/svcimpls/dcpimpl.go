package svcimpls

import (
	"fmt"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

type dcpImpl struct {
}

func (dcp *dcpImpl) Register(h *hookHelper) {
	h.RegisterKvHandler(memd.CmdDcpOpenConnection, dcp.handleOpenDCPConnection)
}

func (dcp *dcpImpl) handleOpenDCPConnection(source mock.KvClient, pak *memd.Packet, start time.Time) {
	fmt.Println("xxx")
}
