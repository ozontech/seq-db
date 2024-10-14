package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	titleKeyword = "message.keyword"
	titlePath    = "message.path"
)

func TestReadMappingWithError(t *testing.T) {
	_, err := LoadMapping("tests/data/mappings/non-existent")
	assert.Error(t, err)
}

func TestReadMappingFromFile(t *testing.T) {
	actual, err := LoadMapping("../tests/data/mappings/logging-new.yaml")
	assert.NoError(t, err)
	expected := Mapping{
		"k8s_pod":       NewSingleType(TokenizerTypeKeyword, "", 0),
		"k8s_namespace": NewSingleType(TokenizerTypeKeyword, "", 0),
		"k8s_container": NewSingleType(TokenizerTypeKeyword, "", 0),
		"request":       NewSingleType(TokenizerTypeText, "", 0),
		"request_uri":   NewSingleType(TokenizerTypePath, "", 0),
		"message": {
			Main: MappingType{Title: "message", TokenizerType: TokenizerTypeText},
			All: []MappingType{
				{Title: "message", TokenizerType: TokenizerTypeText},
				{Title: titleKeyword, TokenizerType: TokenizerTypeKeyword, MaxSize: 18},
			},
		},
		"message.keyword":    NewSingleType(TokenizerTypeKeyword, titleKeyword, 18),
		"someobj":            NewSingleType(TokenizerTypeObject, "", 0),
		"someobj.nested":     NewSingleType(TokenizerTypeKeyword, "", 0),
		"someobj.nestedtext": NewSingleType(TokenizerTypeText, "", 0),
	}
	assert.Equal(t, expected, actual)
}

func TestReadMapping(t *testing.T) {
	testCases := []struct {
		yamlMapping     *mappingYAML
		expectedMapping Mapping
		testName        string
	}{
		{
			testName: "read_mapping_1",
			expectedMapping: Mapping{
				"service": NewSingleType(TokenizerTypeKeyword, "", 0),
				"message": MappingTypes{
					Main: MappingType{Title: "message", TokenizerType: TokenizerTypeText},
					All: []MappingType{
						{Title: "message", TokenizerType: TokenizerTypeText},
						{Title: titleKeyword, TokenizerType: TokenizerTypeKeyword, MaxSize: 255},
						{Title: titlePath, TokenizerType: TokenizerTypePath, MaxSize: 255},
					},
				},
				"message.keyword": NewSingleType(TokenizerTypeKeyword, titleKeyword, 255),
				"message.path":    NewSingleType(TokenizerTypePath, titlePath, 255),
				"operation_name":  NewSingleType(TokenizerTypeKeywordList, "", 0),
			},
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						Types: []MappingTypeIn{
							{Type: FieldTypeText},
							{Title: "keyword", Type: FieldTypeKeyword, Size: 255},
							{Title: "path", Type: FieldTypePath, Size: 255},
						},
						FieldName: "message",
					},
					{
						FieldType: FieldTypeKeyword,
						FieldName: "service",
					},
					{
						FieldType: FieldTypeKeywordList,
						FieldName: "operation_name",
					},
				},
			},
		},
		{
			testName: "read_mapping_nested",
			expectedMapping: Mapping{
				"nested":              NewSingleType(TokenizerTypeObject, "", 0),
				"nested.field":        NewSingleType(TokenizerTypeText, "", 0),
				"nested.nested":       NewSingleType(TokenizerTypeObject, "", 0),
				"nested.nested.field": NewSingleType(TokenizerTypeKeyword, "", 0),
			},
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						FieldType: FieldTypeObject,
						FieldName: "nested",
						Mapping: []mappingItem{
							{
								FieldType: FieldTypeText,
								FieldName: "field",
							},
							{
								FieldType: FieldTypeObject,
								FieldName: "nested",
								Mapping: []mappingItem{
									{
										FieldType: FieldTypeKeyword,
										FieldName: "field",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			testName: "ignore_old_mapping",
			expectedMapping: Mapping{
				"message": MappingTypes{
					Main: MappingType{Title: "message", TokenizerType: TokenizerTypeText},
					All: []MappingType{
						{Title: "message", TokenizerType: TokenizerTypeText},
						{Title: titleKeyword, TokenizerType: TokenizerTypeKeyword, MaxSize: 255},
						{Title: titlePath, TokenizerType: TokenizerTypePath, MaxSize: 255},
					},
				},
				"message.keyword": NewSingleType(TokenizerTypeKeyword, titleKeyword, 255),
				"message.path":    NewSingleType(TokenizerTypePath, titlePath, 255),
			},
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						FieldName: "message",
						FieldType: FieldTypeKeyword,
						Types: []MappingTypeIn{
							{Type: FieldTypeText},
							{Title: "keyword", Type: FieldTypeKeyword, Size: 255},
							{Title: "path", Type: FieldTypePath, Size: 255},
						},
					},
				},
			},
		},
		{
			testName: "case_sensitivity",
			expectedMapping: Mapping{
				"sErViCe": NewSingleType(TokenizerTypeKeyword, "", 0),
			},
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						FieldType: FieldTypeKeyword,
						FieldName: "sErViCe",
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			m := Mapping{}
			assert.NoError(t, readMapping(testCase.yamlMapping, m))
			assert.Equal(t, testCase.expectedMapping, m)
		})
	}
}

func TestReadMappingError(t *testing.T) {
	testCases := []struct {
		yamlMapping   *mappingYAML
		expectedError error
		testName      string
	}{
		{
			testName: "duplicate 1",
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						Types: []MappingTypeIn{
							{Title: "path", Type: FieldTypePath, Size: 255},
							{Title: "path", Type: FieldTypePath, Size: 255},
						},
						FieldName: "message",
					},
				},
			},
			expectedError: fmt.Errorf("duplicate field title in mapping: message.path"),
		},
		{
			testName: "duplicate 2",
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						Types: []MappingTypeIn{
							{Type: FieldTypeText},
							{Type: FieldTypeKeyword},
						},
						FieldName: "message",
					},
				},
			},
			expectedError: fmt.Errorf("duplicate field title in mapping: message._empty_"),
		},
		{
			testName: "unknown field type 1",
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						Types: []MappingTypeIn{
							{Title: titlePath, Type: MappingFieldType("unknown"), Size: 255},
						},
						FieldName: "message",
					},
				},
			},
			expectedError: fmt.Errorf("unknown field type in mapping: unknown"),
		},
		{
			testName: "unknown field type 2",
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						FieldType: MappingFieldType("unknown"),
						FieldName: "message",
					},
				},
			},
			expectedError: fmt.Errorf("unknown field type in mapping: unknown"),
		},
		{
			testName: "no main field",
			yamlMapping: &mappingYAML{
				Mapping: []mappingItem{
					{
						FieldName: "message",
						Types: []MappingTypeIn{
							{Title: "text", Type: FieldTypeText},
							{Title: "keyword", Type: FieldTypeKeyword},
						},
					},
				},
			},
			expectedError: fmt.Errorf("no main field for: message"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			m := Mapping{}
			err := readMapping(testCase.yamlMapping, m)
			assert.Error(t, err)
			assert.Equal(t, testCase.expectedError, err)
		})
	}
}
