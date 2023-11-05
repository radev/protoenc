package protoenc

import (
	"context"
	"testing"

	"github.com/radev/protoenc/internal/mock"
	"github.com/radev/protoenc/internal/test"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
	"go.uber.org/mock/gomock"
)

func TestNewEncoder(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockSchemaRegistryClient(ctrl)

	client.EXPECT().
		CreateSchema(gomock.Any(), "test-value", gomock.Any()).
		Return(sr.SubjectSchema{ID: 1234}, nil)

	enc, err := NewEncoder(context.Background(), client, "test-value", &test.Test{})
	require.NoError(t, err)
	require.Equal(t, 1234, enc.schemaID)

	s := gomock.Eq(sr.Schema{
		Type:   sr.TypeProtobuf,
		Schema: "syntax = \"proto3\";\n\npackage test;\n\nimport \"google/protobuf/timestamp.proto\";\n\noption go_package = \"./test\";\n\nmessage Test {\n  string id = 1;\n\n  google.protobuf.Timestamp create_time = 2;\n}\n\nmessage Test2 {\n  Nested nested = 1;\n\n  message Nested {\n    string value = 1;\n  }\n}\n",
	})

	client.EXPECT().
		CreateSchema(gomock.Any(), "test2-value", s).
		Return(sr.SubjectSchema{ID: 4321}, nil)

	enc2, err := NewEncoder(context.Background(), client, "test2-value", (*test.Test2_Nested)(nil))
	require.NoError(t, err)
	require.Equal(t, 4321, enc2.schemaID)
}

func TestEncoder_Marshal(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewMockSchemaRegistryClient(ctrl)

	client.EXPECT().
		CreateSchema(gomock.Any(), "test-value", gomock.Any()).
		Return(sr.SubjectSchema{ID: 1}, nil)

	enc, err := NewEncoder(context.Background(), client, "test-value", &test.Test{})
	require.NoError(t, err)
	require.Equal(t, 1, enc.schemaID)

	bytes, err := enc.Marshal(&test.Test{})
	require.NoError(t, err)
	// 0 byte for magic byte, 4 bytes for schema ID, zigzag encoded empty array
	require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0x0}, bytes)

	client.EXPECT().
		CreateSchema(gomock.Any(), "test2nested-value", gomock.Any()).
		Return(sr.SubjectSchema{ID: 1}, nil)

	enc2, err := NewEncoder(context.Background(), client, "test2nested-value", &test.Test2_Nested{})
	require.NoError(t, err)
	require.Equal(t, 1, enc.schemaID)

	bytes, err = enc2.Marshal(&test.Test2_Nested{Value: "data"})
	require.NoError(t, err)
	// 0 byte for magic byte, 4 bytes for schema ID, zigzag encoded [1,0] and message
	require.Equal(t, []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0x4, 0x2, 0x0, 0xa, 0x4, 0x64, 0x61, 0x74, 0x61}, bytes)
}
