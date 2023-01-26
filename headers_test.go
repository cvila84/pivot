package pivot

import (
	"fmt"
	"testing"
)

func TestHeadersWalk(t *testing.T) {
	h := newRootHeaders(AlphaSort)
	h.walk("A1").walk("A2").walk("A3")
	h.walk("A1").walk("B2").walk("A3")
	h.walk("A1").walk("B2").walk("B3")
	h.walk("A2").walk("A2").walk("A3")
	h.walk("A2").walk("B2").walk("B3")
	a1a2 := h.walk("A1").walk("A2")
	a1a2s := a1a2.label
	if a1a2s != "A1/A2" {
		t.Fatalf("a1a2.String()=%s!=A1/A2", a1a2s)
	}
	a1b2 := h.walk("A1").walk("B2").sort(ReverseAlphaSort)
	a1b2l := a1b2.labels(false, false)
	if len(a1b2l) != 2 {
		t.Fatalf("len(a1b2l)=%d!=2", len(a1b2l))
	}
	if a1b2l[0] != "A1/B2/B3" {
		t.Fatalf("a1b2l[0]=%s!=A1/B2/B3", a1b2l[0])
	}
	if a1b2l[1] != "A1/B2/A3" {
		t.Fatalf("a1b2l[0]=%s!=A1/B2/A3", a1b2l[1])
	}
	a1 := h.walk("A1")
	a1l := a1.labels(true, true)
	fmt.Printf("%+v\n", a1l)
}
