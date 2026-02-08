package strings

import (
	_ "encoding/base64"
	_ "encoding/hex"
	_ "fmt"
	_ "unicode/utf16"
	_ "unicode/utf8"

	"github.com/tnn1t1s/golars/series"
)

// Encode encodes strings using the specified encoding
func (so *StringOps) Encode(encoding string) (series.Series, error) {
	panic("not implemented")

	// Already UTF-8, return byte representation

	// Check if string is ASCII

}

// Decode decodes strings from the specified encoding
func (so *StringOps) Decode(encoding string) (series.Series, error) {
	panic("not implemented")

	// Assume input is bytes in string form

}

// IsASCII checks if each string contains only ASCII characters
func (so *StringOps) IsASCII() series.Series {
	panic("not implemented")

}

// IsUTF8 checks if each string is valid UTF-8
func (so *StringOps) IsUTF8() series.Series {
	panic("not implemented")

}

// ByteLength returns the byte length of each string
func (so *StringOps) ByteLength() series.Series {
	panic("not implemented")

}

// NormalizeNFD normalizes strings to NFD (Canonical Decomposition)
func (so *StringOps) NormalizeNFD() series.Series {
	panic(
		// TODO: Implement proper Unicode normalization
		// For now, return the original series
		"not implemented")

}

// NormalizeNFC normalizes strings to NFC (Canonical Decomposition followed by Canonical Composition)
func (so *StringOps) NormalizeNFC() series.Series {
	panic(
		// TODO: Implement proper Unicode normalization
		// For now, return the original series
		"not implemented")

}
