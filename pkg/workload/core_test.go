/*
 *
 * core_test.go
 * workload
 *
 * Created by lintao on 2023/7/20 10:49
 * Copyright © 2020-2023 LINTAO. All rights reserved.
 *
 */

package workload

import (
	"github.com/magiconair/properties"
	"testing"
)

func Test_core_buildKeyName(t *testing.T) {

	type args struct {
		keyNum int64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "buildKeyName",
			args: args{keyNum: 227},
			want: "user6284890712318570100",
		},
		{
			name: "buildKeyName1",
			args: args{keyNum: 154},
			want: "user6284898408899967577",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := core{p: properties.MustLoadFiles([]string{"workloads/workloadc"}, properties.UTF8, false)}
			if got := c.buildKeyName(tt.args.keyNum); got != tt.want {
				t.Errorf("buildKeyName() = %v, want %v", got, tt.want)
			}
		})
	}
}
