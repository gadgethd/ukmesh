package coverage

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"image"
	"image/png"
	"io"
	"os"
)

type PublicRFProof struct {
	VisibilityGeneration int64  `json:"visibility_generation"`
	SHA256               string `json:"sha256"`
}

// WritePNG atomically writes img as a PNG to path (write-tmp-then-rename).
func WritePNG(path string, img image.Image) error {
	_, err := writePNG(path, img, 0)
	return err
}

func writePNG(path string, img image.Image, generation int64) (*PublicRFProof, error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp)
	var destination io.Writer = f
	var digest hash.Hash
	if generation > 0 {
		digest = sha256.New()
		destination = io.MultiWriter(f, digest)
	}
	if err := png.Encode(destination, img); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	if generation <= 0 {
		return nil, nil
	}
	return &PublicRFProof{VisibilityGeneration: generation, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func (p *PublicRFProof) matches(path string, generation int64) bool {
	if p == nil || p.VisibilityGeneration != generation || len(p.SHA256) != sha256.Size*2 {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	digest := sha256.New()
	const maxTileBytes = 5 * 1024 * 1024
	bytes, err := io.Copy(digest, io.LimitReader(file, maxTileBytes+1))
	return err == nil && bytes > 0 && bytes <= maxTileBytes && hex.EncodeToString(digest.Sum(nil)) == p.SHA256
}
