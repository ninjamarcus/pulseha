package membership

// MembersSnapshot returns a shallow copy of the current member map while holding
// the read lock, allowing callers to iterate safely without racing mutators.
func (m *MemberList) MembersSnapshot() map[string]*Member {
	m.RLock()
	defer m.RUnlock()

	snapshot := make(map[string]*Member, len(m.Members))
	for id, member := range m.Members {
		snapshot[id] = member
	}
	return snapshot
}
