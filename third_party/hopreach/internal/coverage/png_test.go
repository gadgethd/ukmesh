package coverage

import (
	"crypto/sha256"
	"fmt"
	"image"
	"image/color"
	"os"
	"path/filepath"
	"testing"
)

func TestProgressivePNGProofBindsPublishedBytesAndGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0-0.png")
	img := image.NewNRGBA(image.Rect(0, 0, 2, 2))
	img.SetNRGBA(0, 0, color.NRGBA{R: 100, A: 255})
	proof, err := writePNG(path, img, 42)
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if proof == nil || proof.SHA256 != fmt.Sprintf("%x", sha256.Sum256(content)) || !proof.Matches(path, 42) || proof.Matches(path, 43) {
		t.Fatalf("proof = %+v", proof)
	}
	img.SetNRGBA(0, 0, color.NRGBA{B: 100, A: 255})
	old, err := writePNG(path, img, 0)
	if err != nil || old != nil || proof.Matches(path, 42) {
		t.Fatalf("replaced PNG: legacy proof %+v, err=%v", old, err)
	}
}
