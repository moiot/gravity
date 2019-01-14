package routers

import "testing"

func TestMySQLRoute_GetTarget(t *testing.T) {
	type fields struct {
		RouteMatchers RouteMatchers
		TargetSchema  string
		TargetTable   string
	}
	type args struct {
		msgSchema string
		msgTable  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
		want1  string
	}{
		{
			name:   "empty route should use source schema and table",
			fields: fields{},
			args: args{
				msgSchema: "schema",
				msgTable:  "table",
			},
			want:  "schema",
			want1: "table",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := &MySQLRoute{
				RouteMatchers: tt.fields.RouteMatchers,
				TargetSchema:  tt.fields.TargetSchema,
				TargetTable:   tt.fields.TargetTable,
			}
			got, got1 := route.GetTarget(tt.args.msgSchema, tt.args.msgTable)
			if got != tt.want {
				t.Errorf("MySQLRoute.GetTarget() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("MySQLRoute.GetTarget() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
