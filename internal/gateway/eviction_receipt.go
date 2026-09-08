package gateway

// Only confirmEviction calls this after obtaining a fresh backend commit. A
// recovered receipt remains historical state and MUST be revalidated online.
func (g *evictionOperation) recordReceiptLocked(r evictionColdReceipt) error {
	if !validEvictionReceipt(g.state, r) {
		return errEvictionConfirmation
	}
	if g.state.Receipt != nil {
		if *g.state.Receipt != r {
			return errEvictionConfirmation
		}
		return nil
	}
	frame, err := syncSegmentFrame(r)
	if err != nil || len(frame) > evictionOperationFrameLimit {
		return errEvictionOperation
	}
	st, err := g.f.Stat()
	if err != nil {
		g.poisoned = true
		return errEvictionOperationIO
	}
	if int64(len(frame)) > evictionOperationFileLimit-st.Size() {
		return errEvictionOperation
	}
	if err := g.persist(frame, "receipt"); err != nil {
		return err
	}
	g.state.Receipt = &r
	return nil
}
