//go:build darwin

package runtime

import "testing"

func TestParseDarwinProcessGroupIgnoresZombies(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want bool
	}{
		{name: "active member", body: " 10 20 S+\n 21 21 Z\n", want: true},
		{name: "zombie only", body: " 20 20 Z+\n", want: false},
		{name: "different group", body: " 10 30 R\n", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseDarwinProcessGroup([]byte(test.body), 20)
			if err != nil || got != test.want {
				t.Fatalf("active=%t want=%t err=%v", got, test.want, err)
			}
		})
	}
	if _, err := parseDarwinProcessGroup([]byte("invalid\n"), 20); err == nil {
		t.Fatal("malformed process inventory was accepted")
	}
}
