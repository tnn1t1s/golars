package strings

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/tnn1t1s/golars/series"
)

// Encode encodes strings using the specified encoding
func (so *StringOps) Encode(encoding string) (series.Series, error) {
	switch encoding {
	case "utf8", "utf-8":
		// Already UTF-8, return byte representation
		return applyUnaryOp(so.s, func(str string) interface{} {
			return []byte(str)
		}, "encode_utf8"), nil

	case "utf16", "utf-16":
		return applyUnaryOp(so.s, func(str string) interface{} {
			runes := []rune(str)
			encoded := utf16.Encode(runes)
			bytes := make([]byte, len(encoded)*2)
			for i, u := range encoded {
				bytes[i*2] = byte(u >> 8)
				bytes[i*2+1] = byte(u)
			}
			return bytes
		}, "encode_utf16"), nil

	case "base64":
		return applyUnaryOp(so.s, func(str string) interface{} {
			return base64.StdEncoding.EncodeToString([]byte(str))
		}, "encode_base64"), nil

	case "hex":
		return applyUnaryOp(so.s, func(str string) interface{} {
			return hex.EncodeToString([]byte(str))
		}, "encode_hex"), nil

	case "ascii":
		return applyUnaryOpWithError(so.s, func(str string) (interface{}, error) {
			// Check if string is ASCII
			for i := 0; i < len(str); i++ {
				if str[i] >= 128 {
					return nil, fmt.Errorf("non-ASCII character at position %d", i)
				}
			}
			return []byte(str), nil
		}, "encode_ascii")

	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// Decode decodes strings from the specified encoding
func (so *StringOps) Decode(encoding string) (series.Series, error) {
	switch encoding {
	case "utf8", "utf-8":
		return applyUnaryOpWithError(so.s, func(str string) (interface{}, error) {
			// Assume input is bytes in string form
			if !utf8.ValidString(str) {
				return nil, fmt.Errorf("invalid UTF-8 sequence")
			}
			return str, nil
		}, "decode_utf8")

	case "base64":
		return applyUnaryOpWithError(so.s, func(str string) (interface{}, error) {
			decoded, err := base64.StdEncoding.DecodeString(str)
			if err != nil {
				return nil, err
			}
			return string(decoded), nil
		}, "decode_base64")

	case "hex":
		return applyUnaryOpWithError(so.s, func(str string) (interface{}, error) {
			decoded, err := hex.DecodeString(str)
			if err != nil {
				return nil, err
			}
			return string(decoded), nil
		}, "decode_hex")

	default:
		return nil, fmt.Errorf("unsupported encoding: %s", encoding)
	}
}

// IsASCII checks if each string contains only ASCII characters
func (so *StringOps) IsASCII() series.Series {
	return applyUnaryBoolOp(so.s, func(str string) bool {
		for i := 0; i < len(str); i++ {
			if str[i] >= 128 {
				return false
			}
		}
		return true
	}, "is_ascii")
}

// IsUTF8 checks if each string is valid UTF-8
func (so *StringOps) IsUTF8() series.Series {
	return applyUnaryBoolOp(so.s, func(str string) bool {
		return utf8.ValidString(str)
	}, "is_utf8")
}

// ByteLength returns the byte length of each string
func (so *StringOps) ByteLength() series.Series {
	return applyUnaryOp(so.s, func(str string) interface{} {
		return int32(len(str))
	}, "byte_length")
}

// NormalizeNFD normalizes strings to NFD (Canonical Decomposition)
func (so *StringOps) NormalizeNFD() series.Series {
	// TODO: Implement proper Unicode normalization
	// For now, return the original series
	return so.s
}

// NormalizeNFC normalizes strings to NFC (Canonical Decomposition followed by Canonical Composition)
func (so *StringOps) NormalizeNFC() series.Series {
	// TODO: Implement proper Unicode normalization
	// For now, return the original series
	return so.s
}
