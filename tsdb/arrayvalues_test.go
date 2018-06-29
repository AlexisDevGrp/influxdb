package tsdb_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb"
)

func makeBooleanArray(v ...interface{}) *tsdb.BooleanArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := tsdb.NewBooleanArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = v[i+1].(bool)
	}
	return a
}

func makeFloatArray(v ...interface{}) *tsdb.FloatArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := tsdb.NewFloatArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = v[i+1].(float64)
	}
	return a
}

func makeIntegerArray(v ...interface{}) *tsdb.IntegerArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := tsdb.NewIntegerArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = int64(v[i+1].(int))
	}
	return a
}

func makeUnsignedArray(v ...interface{}) *tsdb.UnsignedArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := tsdb.NewUnsignedArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = uint64(v[i+1].(int))
	}
	return a
}

func makeStringArray(v ...interface{}) *tsdb.StringArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := tsdb.NewStringArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = v[i+1].(string)
	}
	return a
}

func TestBooleanArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *tsdb.BooleanArray
	}{

		{
			name: "empty a",

			a:   makeBooleanArray(),
			b:   makeBooleanArray(1, true, 2, true),
			exp: makeBooleanArray(1, true, 2, true),
		},
		{
			name: "empty b",

			a:   makeBooleanArray(1, true, 2, true),
			b:   makeBooleanArray(),
			exp: makeBooleanArray(1, true, 2, true),
		},
		{
			name: "b replaces a",

			a: makeBooleanArray(1, true),
			b: makeBooleanArray(
				0, false,
				1, false, // overwrites a
				2, false,
				3, false,
				4, false,
			),
			exp: makeBooleanArray(0, false, 1, false, 2, false, 3, false, 4, false),
		},
		{
			name: "b replaces partial a",

			a: makeBooleanArray(1, true, 2, true, 3, true, 4, true),
			b: makeBooleanArray(
				1, false, // overwrites a
				2, false, // overwrites a
			),
			exp: makeBooleanArray(
				1, false, // overwrites a
				2, false, // overwrites a
				3, true,
				4, true,
			),
		},
		{
			name: "b replaces all a",

			a:   makeBooleanArray(1, true, 2, true, 3, true, 4, true),
			b:   makeBooleanArray(1, false, 2, false, 3, false, 4, false),
			exp: makeBooleanArray(1, false, 2, false, 3, false, 4, false),
		},
		{
			name: "b replaces a interleaved",
			a:    makeBooleanArray(0, true, 1, true, 2, true, 3, true, 4, true),
			b:    makeBooleanArray(0, false, 2, false, 4, false),
			exp:  makeBooleanArray(0, false, 1, true, 2, false, 3, true, 4, false),
		},
		{
			name: "b merges a interleaved",
			a:    makeBooleanArray(0, true, 2, true, 4, true),
			b:    makeBooleanArray(1, false, 3, false, 5, false),
			exp:  makeBooleanArray(0, true, 1, false, 2, true, 3, false, 4, true, 5, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}

func TestFloatArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *tsdb.FloatArray
	}{

		{
			name: "empty a",

			a:   makeFloatArray(),
			b:   makeFloatArray(1, 1.1, 2, 2.1),
			exp: makeFloatArray(1, 1.1, 2, 2.1),
		},
		{
			name: "empty b",

			a:   makeFloatArray(1, 1.0, 2, 2.0),
			b:   makeFloatArray(),
			exp: makeFloatArray(1, 1.0, 2, 2.0),
		},
		{
			name: "b replaces a",

			a: makeFloatArray(1, 1.0),
			b: makeFloatArray(
				0, 0.1,
				1, 1.1, // overwrites a
				2, 2.1,
				3, 3.1,
				4, 4.1,
			),
			exp: makeFloatArray(0, 0.1, 1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
		},
		{
			name: "b replaces partial a",

			a: makeFloatArray(1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b: makeFloatArray(
				1, 1.1, // overwrites a
				2, 2.1, // overwrites a
			),
			exp: makeFloatArray(
				1, 1.1, // overwrites a
				2, 2.1, // overwrites a
				3, 3.0,
				4, 4.0,
			),
		},
		{
			name: "b replaces all a",

			a:   makeFloatArray(1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b:   makeFloatArray(1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
			exp: makeFloatArray(1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
		},
		{
			name: "b replaces a interleaved",
			a:    makeFloatArray(0, 0.0, 1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b:    makeFloatArray(0, 0.1, 2, 2.1, 4, 4.1),
			exp:  makeFloatArray(0, 0.1, 1, 1.0, 2, 2.1, 3, 3.0, 4, 4.1),
		},
		{
			name: "b merges a interleaved",
			a:    makeFloatArray(0, 0.0, 2, 2.0, 4, 4.0),
			b:    makeFloatArray(1, 1.1, 3, 3.1, 5, 5.1),
			exp:  makeFloatArray(0, 0.0, 1, 1.1, 2, 2.0, 3, 3.1, 4, 4.0, 5, 5.1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}
