package sqs

import "testing"

func Test_normalizeTag(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "normalize already normalized tag",
			args: args{
				name: "test",
			},
			want: "test",
		},
		{
			name: "normalize uppercase tag",
			args: args{
				name: "TEst",
			},
			want: "test",
		},
		{
			name: "normalize dot tag",
			args: args{
				name: "test.test",
			},
			want: "test_test",
		},
		{
			name: "normalize slash tag",
			args: args{
				name: "test/test",
			},
			want: "test_test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeTag(tt.args.name); got != tt.want {
				t.Errorf("normalizeTag() = %v, want %v", got, tt.want)
			}
		})
	}
}
