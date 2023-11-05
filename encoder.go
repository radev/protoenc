package protoenc

import (
	"context"
	"errors"
	"slices"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// ErrWrongMessage is returned when Marshal/MarshalAppend receive
	// a message different from one in the constructor.
	ErrWrongMessage = errors.New("unexpected message type")
)

func NewEncoder(ctx context.Context, client SchemaRegistryClient, subject string, msg proto.Message) (*Encoder, error) {
	msgDesc := msg.ProtoReflect().Descriptor()
	fileDesc := msgDesc.ParentFile()

	sources := make(map[string]string)
	err := gatherImportSources(fileDesc, sources)
	if err != nil {
		return nil, err
	}

	source, err := fileDescriptorToSource(fileDesc)
	if err != nil {
		return nil, err
	}

	schema := sr.Schema{
		Type:   sr.TypeProtobuf,
		Schema: source,
	}

	// register all the references depth-first.
	schema.References, err = createRefs(ctx, client, fileDesc.Imports(), sources)
	if err != nil {
		return nil, err
	}

	// Now register the schema.
	ss, err := client.CreateSchema(ctx, subject, schema)
	if err != nil {
		return nil, err
	}

	return &Encoder{
		ch:       &sr.ConfluentHeader{},
		schemaID: ss.ID,
		index:    getMessageIndex(msgDesc),
		fqname:   msgDesc.FullName(),
	}, nil
}

func createRefs(ctx context.Context, client SchemaRegistryClient, imports protoreflect.FileImports, sources map[string]string) ([]sr.SchemaReference, error) {
	if imports.Len() == 0 {
		return nil, nil
	}
	refs := make([]sr.SchemaReference, 0, imports.Len())
	for i := 0; i < imports.Len(); i++ {
		imp := imports.Get(i)
		name := imp.Path()
		if ignoredSource(name) {
			continue
		}

		// Recursively add imports of import.
		importRefs, err := createRefs(ctx, client, imp.Imports(), sources)
		if err != nil {
			return nil, err
		}

		source, ok := sources[name]
		if !ok {
			return nil, errors.New("missing source for import " + name)
		}
		// Register the schema.
		ss, err := client.CreateSchema(ctx, name, sr.Schema{
			Type:       sr.TypeProtobuf,
			Schema:     source,
			References: importRefs,
		})
		if err != nil {
			return nil, err
		}

		refs = append(refs, sr.SchemaReference{
			Name:    name,
			Subject: ss.Subject,
			Version: ss.Version,
		})
	}
	if len(refs) == 0 {
		return nil, nil
	}
	return refs, nil
}

type Encoder struct {
	ch       *sr.ConfluentHeader
	schemaID int                   // Schema ID in the schema registry
	index    []int                 // Index of the message in the protobuf file
	fqname   protoreflect.FullName // Fully qualified protobuf message name
}

// Marshal returns the confluent wire-format encoding of msg.
// See https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html#wire-format
// for details.
func (e *Encoder) Marshal(msg proto.Message) ([]byte, error) {
	return e.MarshalAppend(nil, msg)
}

// MarshalAppend appends the confluent wire-format encoding of msg with stored recordID and index to b,
// returning the result.
// See https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/index.html#wire-format
// for details.
func (e *Encoder) MarshalAppend(b []byte, msg proto.Message) ([]byte, error) {
	// Check if the message is the same as the one in the constructor.
	if e.fqname != msg.ProtoReflect().Descriptor().FullName() {
		return nil, ErrWrongMessage
	}
	// Append the header.
	b, err := e.ch.AppendEncode(b, e.schemaID, e.index)
	if err != nil {
		return nil, err
	}
	// Append the message.
	return proto.MarshalOptions{}.MarshalAppend(b, msg)
}

//go:generate go run go.uber.org/mock/mockgen@v0.3.0 -destination=internal/mock/encoder.go -package mock -source=encoder.go
type SchemaRegistryClient interface {
	CreateSchema(ctx context.Context, subject string, s sr.Schema) (sr.SubjectSchema, error)
}

func fileDescriptorToSource(fileDesc protoreflect.FileDescriptor) (string, error) {
	d, err := desc.WrapFile(fileDesc)
	if err != nil {
		return "", err
	}
	p := protoprint.Printer{OmitComments: protoprint.CommentsAll}

	return p.PrintProtoToString(d)
}

func gatherImportSources(fileDesc protoreflect.FileDescriptor, imports map[string]string) error {
	for i := 0; i < fileDesc.Imports().Len(); i++ {
		importDesc := fileDesc.Imports().Get(i)
		if ignoredSource(importDesc.Path()) {
			continue
		}
		source, err := fileDescriptorToSource(importDesc)
		if err != nil {
			return err
		}
		imports[importDesc.Path()] = source

		// Recursively add imports of imports.
		err = gatherImportSources(importDesc, imports)
		if err != nil {
			return err
		}
	}
	return nil
}

var ignoredSources = map[string]struct{}{
	"confluent/meta.proto":                 {},
	"confluent/type/decimal.proto":         {},
	"google/type/calendar_period.proto":    {},
	"google/type/color.proto":              {},
	"google/type/date.proto":               {},
	"google/type/datetime.proto":           {},
	"google/type/dayofweek.proto":          {},
	"google/type/decimal.proto":            {},
	"google/type/expr.proto":               {},
	"google/type/fraction.proto":           {},
	"google/type/interval.proto":           {},
	"google/type/latlng.proto":             {},
	"google/type/money.proto":              {},
	"google/type/month.proto":              {},
	"google/type/phone_number.proto":       {},
	"google/type/postal_address.proto":     {},
	"google/type/quaternion.proto":         {},
	"google/type/timeofday.proto":          {},
	"google/protobuf/any.proto":            {},
	"google/protobuf/api.proto":            {},
	"google/protobuf/descriptor.proto":     {},
	"google/protobuf/duration.proto":       {},
	"google/protobuf/empty.proto":          {},
	"google/protobuf/field_mask.proto":     {},
	"google/protobuf/source_context.proto": {},
	"google/protobuf/struct.proto":         {},
	"google/protobuf/timestamp.proto":      {},
	"google/protobuf/type.proto":           {},
	"google/protobuf/wrappers.proto":       {},
}

func ignoredSource(path string) bool {
	_, ok := ignoredSources[path]
	return ok
}

// getMessageIndex returns the index of the message in the protobuf file.
// Example: for the following protobuf file:
//
//	syntax = "proto3";
//	package foo;
//	message MessageA {
//	  message Message B {
//	    message Message C {
//	      ...
//	    }
//	  }
//	  message Message D {
//	    ...
//	  }
//	  message Message E {
//	    message Message F {
//	      ...
//	    }
//	    message Message G {
//	      ...
//	    }
//	    ...
//	  }
//	  ...
//	}
//	message MessageH {
//	  message MessageI {
//	    ...
//	  }
//	}
//
// getMessageIndex(desc) returns []int{0} for the message A.
// getMessageIndex(desc) returns []int{0, 0} for the message B.
// getMessageIndex(desc) returns []int{0, 2, 1} for the message G.
// getMessageIndex(desc) returns []int{1, 0} for the message I.
func getMessageIndex(desc protoreflect.Descriptor) []int {
	var index []int
	for desc != nil {
		index = append(index, desc.Index())
		desc = desc.Parent()
	}
	slices.Reverse(index)
	// Cut the first element, which is the index of the file descriptor.
	return index[1:]
}
