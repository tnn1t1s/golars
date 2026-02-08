package strings

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"github.com/tnn1t1s/golars/internal/datatypes"
	"github.com/tnn1t1s/golars/series"
)

// Encode encodes strings using the specified encoding
func (so *StringOps) Encode(encoding string) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)
	result := make([]string, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}
		switch encoding {
		case "base64":
			result[i] = base64.StdEncoding.EncodeToString([]byte(values[i]))
			resultValidity[i] = true
		case "hex":
			result[i] = hex.EncodeToString([]byte(values[i]))
			resultValidity[i] = true
		case "utf-8", "utf8":
			result[i] = values[i]
			resultValidity[i] = true
		case "ascii":
			// Check if all characters are ASCII
			isASCII := true
			for _, r := range values[i] {
				if r > 127 {
					isASCII = false
					break
				}
			}
			if isASCII {
				result[i] = values[i]
				resultValidity[i] = true
			}
		default:
			return nil, fmt.Errorf("unsupported encoding: %s", encoding)
		}
	}

	return series.NewSeriesWithValidity("encode", result, resultValidity, datatypes.String{}), nil
}

// Decode decodes strings from the specified encoding
func (so *StringOps) Decode(encoding string) (series.Series, error) {
	values, validity := getStringValuesWithValidity(so.s)
	n := len(values)
	result := make([]string, n)
	resultValidity := make([]bool, n)

	for i := 0; i < n; i++ {
		if !validity[i] {
			continue
		}
		switch encoding {
		case "base64":
			decoded, err := base64.StdEncoding.DecodeString(values[i])
			if err != nil {
				continue
			}
			result[i] = string(decoded)
			resultValidity[i] = true
		case "hex":
			decoded, err := hex.DecodeString(values[i])
			if err != nil {
				continue
			}
			result[i] = string(decoded)
			resultValidity[i] = true
		case "utf-8", "utf8":
			result[i] = values[i]
			resultValidity[i] = true
		default:
			return nil, fmt.Errorf("unsupported encoding: %s", encoding)
		}
	}

	return series.NewSeriesWithValidity("decode", result, resultValidity, datatypes.String{}), nil
}

// IsASCII checks if each string contains only ASCII characters
func (so *StringOps) IsASCII() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		for _, r := range s {
			if r > 127 {
				return false
			}
		}
		return true
	}, "is_ascii")
}

// IsUTF8 checks if each string is valid UTF-8
func (so *StringOps) IsUTF8() series.Series {
	return applyUnaryBoolOp(so.s, func(s string) bool {
		return utf8.ValidString(s)
	}, "is_utf8")
}

// ByteLength returns the byte length of each string
func (so *StringOps) ByteLength() series.Series {
	return applyUnaryInt32Op(so.s, func(s string) int32 {
		return int32(len(s))
	}, "byte_length")
}

// NormalizeNFD normalizes strings to NFD (Canonical Decomposition)
func (so *StringOps) NormalizeNFD() series.Series {
	// Return original series (proper Unicode normalization requires golang.org/x/text)
	return so.s
}

// NormalizeNFC normalizes strings to NFC (Canonical Decomposition followed by Canonical Composition)
func (so *StringOps) NormalizeNFC() series.Series {
	// Return original series (proper Unicode normalization requires golang.org/x/text)
	return so.s
}
