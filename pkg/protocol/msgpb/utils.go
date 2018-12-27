package msgpb

func NewMySQLNULLData() *MySQLColumnData {
	pbData := MySQLColumnData{}
	pbData.Value = &MySQLColumnValue{IsNull: true}
	return &pbData
}

func NewMySQLValidColumnDataInt64(d string) *MySQLColumnData {
	pbData := MySQLColumnData{}
	pbData.ColumnType = MySQLColumnType_INT64
	pbData.Value = &MySQLColumnValue{IsNull: false, ValueString: d}
	return &pbData
}

func NewMySQLValidColumnDataString(d string) *MySQLColumnData {
	pbData := MySQLColumnData{}
	pbData.ColumnType = MySQLColumnType_STRING
	pbData.Value = &MySQLColumnValue{IsNull: false, ValueString: d}
	return &pbData
}

func NewMySQLValidColumnDataMapInt64(d map[string]string) map[string]*MySQLColumnData {
	pbDataMap := make(map[string]*MySQLColumnData)
	for k, _ := range d {
		pbDataMap[k] = &MySQLColumnData{
			ColumnType: MySQLColumnType_INT64,
			Value: &MySQLColumnValue{
				IsNull:      false,
				ValueString: d[k],
			},
		}
	}
	return pbDataMap
}
