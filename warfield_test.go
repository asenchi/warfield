package main

import (
	"testing"
)

var logmessages = []struct {
	raw string
	out []string
}{
	{`<173>1 2011-11-10T11:10:53-08:00 Nov 10 10:58:53 face-argon nginx: 198.228.192.8 - - [10/Nov/2011:10:58:53 -0800] "-" 400 0 "-" "-" -`,
		[]string{`<173>1 2011-11-10T11:10:53-08:00 Nov 10 10:58:53 face-argon nginx: 198.228.192.8 - - [10/Nov/2011:10:58:53 -0800] "-" 400 0 "-" "-" -`, "173", "\"-\" 400 0 \"-\" \"-\"", "-"}},
	{`<173>1 2011-11-10T11:10:53-08:00 Nov 10 10:58:53 face-argon nginx: 216.165.95.69 - - [10/Nov/2011:10:58:53 -0800] "GET /images/merchants_images_other_32x32_1x.png HTTP/1.1" 200 556 "-" "LevelUp 2.2.1 (iPhone; iPhone OS 5.0; en_US)" www.thelevelup.com`,
		[]string{`<173>1 2011-11-10T11:10:53-08:00 Nov 10 10:58:53 face-argon nginx: 216.165.95.69 - - [10/Nov/2011:10:58:53 -0800] "GET /images/merchants_images_other_32x32_1x.png HTTP/1.1" 200 556 "-" "LevelUp 2.2.1 (iPhone; iPhone OS 5.0; en_US)" www.thelevelup.com`, "173", "\"GET /images/merchants_images_other_32x32_1x.png HTTP/1.1\" 200 556 \"-\" \"LevelUp 2.2.1 (iPhone; iPhone OS 5.0; en_US)\"", "www.thelevelup.com"}},
}

func TestRegex(t *testing.T) {
	for _, log := range logmessages {
		groups := Re.FindStringSubmatch(log.raw)

		for i, _ := range groups {
			if groups[i] != log.out[i] {
				t.Errorf("Group[%d] didn't match\n\tGot: %v\n\tWant:%v\n", i, log.out[i], groups[i])
			} else {
				t.Logf("Passed: group[%d] = %v\n", i, log.out[i])
			}
		}
	}
}
