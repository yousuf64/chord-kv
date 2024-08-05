package router

type ErrorReply struct {
	Status int
}

func (e *ErrorReply) Error() string {
	return ""
}

type SetRequest struct {
	Key     string `json:"key"`
	Content string `json:"content"`
}

type GetReply struct {
	Size int64  `json:"size"`
	Hash string `json:"hash"`
}
