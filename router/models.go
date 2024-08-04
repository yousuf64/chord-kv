package router

type ErrorReply struct {
	Status int
}

func (e *ErrorReply) Error() string {
	return ""
}

type SetRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetReply struct {
	Value string `json:"value"`
}
