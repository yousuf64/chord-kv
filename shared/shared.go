package shared

func Between(id, start, end uint64) bool {
	if start < end {
		return id > start && id <= end // 3 ...5 8 9... 12
	}
	return id > start || id <= end
}
