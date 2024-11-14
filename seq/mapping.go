package seq

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

const PathDelim = "."

var TestMapping = Mapping{
	"service":  NewSingleType(TokenizerTypeKeyword, "", 0),
	"span_id":  NewSingleType(TokenizerTypeKeyword, "", 0),
	"trace_id": NewSingleType(TokenizerTypeKeyword, "", 0),
	"message": {
		Main: MappingType{TokenizerType: TokenizerTypeText},
		All: []MappingType{
			{Title: "message", TokenizerType: TokenizerTypeText},
			{Title: "message.keyword", TokenizerType: TokenizerTypeKeyword, MaxSize: 18},
		},
	},
	"message.keyword":     NewSingleType(TokenizerTypeKeyword, "message.keyword", 18),
	"text":                NewSingleType(TokenizerTypeText, "", 0),
	"k8s_pod":             NewSingleType(TokenizerTypeKeyword, "", 0),
	"level":               NewSingleType(TokenizerTypeKeyword, "", 0),
	"traceID":             NewSingleType(TokenizerTypeKeyword, "", 0),
	"request_uri":         NewSingleType(TokenizerTypePath, "", 0),
	"tags":                NewSingleType(TokenizerTypeTags, "", 0),
	"process":             NewSingleType(TokenizerTypeObject, "", 0),
	"process.tags":        NewSingleType(TokenizerTypeTags, "", 0),
	"process.serviceName": NewSingleType(TokenizerTypeKeyword, "", 0),
	"tags.sometag":        NewSingleType(TokenizerTypeKeyword, "", 0),
	"request_duration":    NewSingleType(TokenizerTypeKeyword, "", 0),
	"_exists_":            NewSingleType(TokenizerTypeKeyword, "", 0),

	"m": NewSingleType(TokenizerTypeKeyword, "", 0),
}

type MappingFieldType string

const (
	FieldTypeText        MappingFieldType = "text"
	FieldTypeKeyword     MappingFieldType = "keyword"
	FieldTypeKeywordList MappingFieldType = "keyword-list"
	FieldTypePath        MappingFieldType = "path"

	FieldTypeObject MappingFieldType = "object"
	FieldTypeTags   MappingFieldType = "tags"
	FieldTypeNested MappingFieldType = "nested"
)

type MappingTypeIn struct {
	Title string           `yaml:"title"`
	Type  MappingFieldType `yaml:"type"`
	Size  int              `yaml:"size"`
}

type mappingItem struct {
	Types     []MappingTypeIn  `yaml:"types"`
	FieldType MappingFieldType `yaml:"type"`
	FieldName string           `yaml:"name"`
	Mapping   []mappingItem    `yaml:"mapping-list"`
}

type mappingYAML struct {
	Mapping []mappingItem `yaml:"mapping-list"`
}

type MappingType struct {
	Title         string
	TokenizerType TokenizerType
	MaxSize       int
}

type MappingTypes struct {
	// Main - original field, used in "read" requests to get tokenizer type for field from search query
	Main MappingType
	// All - all fields including main one, used in "write" requests to index tokens for each type
	All []MappingType
}

// Mapping - maps fields to tokenizers. For fields with multiple types there must be a key for each type
type Mapping map[string]MappingTypes

type FieldMapping Mapping

func convertMapping(yamlMapping []mappingItem, finalMapping Mapping, path string) error {
	for _, el := range yamlMapping {
		fn := el.FieldName
		if path != "" {
			fn = path + PathDelim + fn
		}

		if len(el.Types) > 0 {
			err := convertMappingWithMultipleTypes(fn, el, finalMapping)
			if err != nil {
				return err
			}
		} else if el.FieldName != "" {
			// support old mapping structure
			if v, ok := NamesToTokenTypes[string(el.FieldType)]; ok {
				finalMapping[fn] = NewSingleType(v, "", 0)
			} else {
				return fmt.Errorf("unknown field type in mapping: %s", el.FieldType)
			}
		} else {
			panic("BUG: no field type in mapping")
		}

		if el.FieldType == FieldTypeObject || el.FieldType == FieldTypeTags || el.FieldType == FieldTypeNested {
			if err := convertMapping(el.Mapping, finalMapping, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertMappingWithMultipleTypes(fn string, el mappingItem, finalMapping Mapping) error {
	types := make([]MappingType, 0, len(el.Types))
	seen := make(map[string]struct{})

	mappingTypes := MappingTypes{}

	for _, t := range el.Types {
		if _, ok := seen[t.Title]; ok {
			if t.Title == "" {
				t.Title = "_empty_"
			}
			return fmt.Errorf("duplicate field title in mapping: %s.%s", fn, t.Title)
		}

		v, ok := NamesToTokenTypes[string(t.Type)]
		if !ok {
			return fmt.Errorf("unknown field type in mapping: %s", t.Type)
		}

		seen[t.Title] = struct{}{}

		title := t.Title
		if title == "" {
			mappingTypes.Main = MappingType{Title: fn, TokenizerType: v, MaxSize: t.Size}
			title = fn
		} else {
			title = fn + PathDelim + t.Title
			finalMapping[title] = NewSingleType(v, title, t.Size)
		}

		types = append(types, MappingType{Title: title, TokenizerType: v, MaxSize: t.Size})
	}

	if mappingTypes.Main.TokenizerType == TokenizerTypeNoop {
		return fmt.Errorf("no main field for: %s", fn)
	}

	mappingTypes.All = types
	finalMapping[fn] = mappingTypes

	return nil
}

func readMapping(mapYAML *mappingYAML, finalMapping Mapping) error {
	if len(mapYAML.Mapping) == 0 {
		return errors.New("invalid mapping provided")
	}

	err := convertMapping(mapYAML.Mapping, finalMapping, "")
	if err != nil {
		return err
	}

	return nil
}

func LoadMapping(file string) (Mapping, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	mapYAML := &mappingYAML{}
	err = yaml.Unmarshal(data, mapYAML)
	if err != nil {
		return nil, err
	}
	res := Mapping{}
	return res, readMapping(mapYAML, res)
}

type RawMapping struct {
	rawMapping []byte
}

func NewRawMapping(mapping Mapping) *RawMapping {
	return &RawMapping{
		rawMapping: marshalMapping(mapping),
	}
}

func marshalMapping(initialMapping Mapping) []byte {
	convertedMapping := make(map[string]string)
	for k, v := range initialMapping {
		convertedMapping[k] = TokenTypesToNames[v.Main.TokenizerType]
	}
	b, err := json.Marshal(convertedMapping)
	if err != nil {
		panic(fmt.Errorf("BUG: can't marshal mapping: %s", err))
	}
	return b
}

// GetRawMappingBytes returns raw mapping represented as json stored in bytes
func (a *RawMapping) GetRawMappingBytes() []byte {
	return a.rawMapping
}

func NewSingleType(tokenizerType TokenizerType, title string, maxSize int) MappingTypes {
	return MappingTypes{
		Main: MappingType{Title: title, TokenizerType: tokenizerType, MaxSize: maxSize},
		All:  []MappingType{{Title: title, TokenizerType: tokenizerType, MaxSize: maxSize}},
	}
}
