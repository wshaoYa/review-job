package job

// Msg kafka的message类型
type Msg struct {
	Type     string `json:"type"`
	Database string `json:"database"`
	Table    string `json:"table"`
	IsDdl    bool   `json:"isDdl"`
	Data     []map[string]interface{}
}
