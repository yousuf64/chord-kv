package router

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetReply struct {
	Value string `json:"value"`
}
