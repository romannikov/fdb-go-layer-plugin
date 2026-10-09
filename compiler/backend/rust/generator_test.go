package rust

import (
	"strings"
	"testing"

	"github.com/romannikov/fdb-layer/compiler/ir"
)

func TestGenerateRustRepository_StandardAndAtomics(t *testing.T) {
	msg := ir.MessageSpec{
		Name: "Counter",
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
			{ProtoName: "value", GoName: "Value", Number: 2, Kind: ir.KindInt64, Mutation: ir.MutationAdd},
			{ProtoName: "max_value", GoName: "MaxValue", Number: 3, Kind: ir.KindInt64, Mutation: ir.MutationMax},
			{ProtoName: "min_value", GoName: "MinValue", Number: 4, Kind: ir.KindInt64, Mutation: ir.MutationMin},
		},
	}

	var buf BufferPrinter
	GenerateFile(&buf, "atomic.proto", []ir.MessageSpec{msg})
	code := buf.String()

	expectedSubstrings := []string{
		"pub struct CounterRepository",
		"pub async fn create(",
		"pub async fn get(",
		"pub async fn set(",
		"pub async fn delete(",
		"pub async fn batch_get_counter(",
		"pub async fn batch_get_counter_by_pk(",
		"pub async fn list_counter(",
		"pub async fn add_counter_value(",
		"pub async fn max_counter_max_value(",
		"pub async fn min_counter_min_value(",
		"pub async fn set_counter_value(",
		"pub async fn set_counter_max_value(",
		"pub async fn set_counter_min_value(",
		"fdb_layer::MutationType::Add",
		"fdb_layer::MutationType::Max",
		"fdb_layer::MutationType::Min",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code to contain %q, got:\n%s", exp, code)
		}
	}
}

func TestGenerateRustRepository_Queue(t *testing.T) {
	msg := ir.MessageSpec{
		Name:    "TaskMessage",
		IsQueue: true,
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "queue_name", GoName: "QueueName", Number: 1, Kind: ir.KindString},
			{ProtoName: "versionstamp", GoName: "Versionstamp", Number: 3, Kind: ir.KindBytes, IsVersionstamp: true},
			{ProtoName: "shard_id", GoName: "ShardId", Number: 2, Kind: ir.KindUint32, IsUnsigned: true},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "queue_name", GoName: "QueueName", Number: 1, Kind: ir.KindString},
			{ProtoName: "shard_id", GoName: "ShardId", Number: 2, Kind: ir.KindUint32, IsUnsigned: true},
			{ProtoName: "versionstamp", GoName: "Versionstamp", Number: 3, Kind: ir.KindBytes, IsVersionstamp: true},
			{ProtoName: "payload", GoName: "Payload", Number: 4, Kind: ir.KindBytes},
		},
	}

	var buf BufferPrinter
	GenerateMessage(&buf, msg)
	code := buf.String()

	expectedSubstrings := []string{
		"pub struct TaskMessageRepository",
		"pub async fn enqueue(",
		"pub async fn dequeue(",
		"pub async fn dequeue_batch(",
		"self.store.prepare_user_version(tr)?;",
		"fdb_layer::Versionstamp::incomplete(self.store.next_user_version())",
		"(entity.shard_id as u64)",
		"fdb_layer::MutationType::SetVersionstampedKey",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust queue code to contain %q, got:\n%s", exp, code)
		}
	}
}

func TestGenerateRustRepository_BytesPrimaryKeyAndSecondaryIndexes(t *testing.T) {
	msg := ir.MessageSpec{
		Name: "BlobRecord",
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "raw_id", GoName: "RawId", Number: 1, Kind: ir.KindBytes},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "raw_id", GoName: "RawId", Number: 1, Kind: ir.KindBytes},
			{ProtoName: "hash", GoName: "Hash", Number: 2, Kind: ir.KindBytes},
			{ProtoName: "chunks", GoName: "Chunks", Number: 3, Kind: ir.KindBytes, IsRepeated: true},
		},
		SecondaryIndexes: []ir.SecondaryIndexSpec{
			{
				Fields: []ir.FieldSpec{
					{ProtoName: "hash", GoName: "Hash", Number: 2, Kind: ir.KindBytes},
				},
				IndexID: 101,
			},
			{
				Fields: []ir.FieldSpec{
					{ProtoName: "chunks", GoName: "Chunks", Number: 3, Kind: ir.KindBytes, IsRepeated: true},
				},
				IsFanOut:    true,
				FanOutField: ir.FieldSpec{ProtoName: "chunks", GoName: "Chunks", Number: 3, Kind: ir.KindBytes, IsRepeated: true},
				IndexID:     102,
			},
		},
	}

	var buf BufferPrinter
	GenerateMessage(&buf, msg)
	code := buf.String()

	expectedSubstrings := []string{
		"fdb_layer::Bytes::from(&entity.raw_id[..])",
		"fdb_layer::Bytes::from(&pk[..])",
		"fdb_layer::Bytes::from(&entity.hash[..])",
		"fdb_layer::Bytes::from(&old.hash[..])",
		"fdb_layer::Bytes::from(&hash[..])",
		"fdb_layer::Bytes::from(&item[..])",
		"pub async fn query_blob_record_by_hash(",
		"pub async fn query_blob_record_by_chunks(",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code for bytes PK/indexes to contain %q, got:\n%s", exp, code)
		}
	}
}

func TestGenerateProstMessages_NestedMessagesAndEnums(t *testing.T) {
	topEnums := []ir.EnumSpec{
		{
			Name: "Role",
			Values: []ir.EnumValueSpec{
				{ProtoName: "ROLE_UNSPECIFIED", RustName: "Unspecified", Number: 0},
				{ProtoName: "ROLE_ADMIN", RustName: "Admin", Number: 1},
			},
		},
	}

	helperMsg := ir.MessageSpec{
		Name: "Address",
		Fields: []ir.FieldSpec{
			{ProtoName: "city", GoName: "City", Number: 1, Kind: ir.KindString},
		},
	}

	accountMsg := ir.MessageSpec{
		Name: "Account",
		PrimaryKeyFields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
		},
		Fields: []ir.FieldSpec{
			{ProtoName: "id", GoName: "Id", Number: 1, Kind: ir.KindString},
			{ProtoName: "role", GoName: "Role", Number: 2, Kind: ir.KindEnum, EnumRustName: "Role", IsEnum: true},
			{ProtoName: "status", GoName: "Status", Number: 3, Kind: ir.KindEnum, EnumRustName: "account::Status", IsEnum: true},
			{ProtoName: "billing_address", GoName: "BillingAddress", Number: 4, Kind: ir.KindMessage, MessageRustName: "Address"},
			{ProtoName: "profile", GoName: "Profile", Number: 5, Kind: ir.KindMessage, MessageRustName: "account::Profile"},
			{ProtoName: "extra_addresses", GoName: "ExtraAddresses", Number: 6, Kind: ir.KindMessage, MessageRustName: "Address", IsRepeated: true},
		},
		NestedEnums: []ir.EnumSpec{
			{
				Name: "Status",
				Values: []ir.EnumValueSpec{
					{ProtoName: "STATUS_UNSPECIFIED", RustName: "Unspecified", Number: 0},
					{ProtoName: "STATUS_ACTIVE", RustName: "Active", Number: 1},
				},
			},
		},
		NestedMessages: []ir.MessageSpec{
			{
				Name: "Profile",
				Fields: []ir.FieldSpec{
					{ProtoName: "bio", GoName: "Bio", Number: 1, Kind: ir.KindString},
					{ProtoName: "home_address", GoName: "HomeAddress", Number: 2, Kind: ir.KindMessage, MessageRustName: "Address"},
					{ProtoName: "status", GoName: "Status", Number: 3, Kind: ir.KindEnum, EnumRustName: "account::Status", IsEnum: true},
				},
			},
		},
	}

	var buf BufferPrinter
	GenerateFileWithOptions(&buf, "account.proto", []ir.MessageSpec{helperMsg, accountMsg}, GenerateOptions{
		GenerateMessages: true,
		Enums:            topEnums,
	})
	code := buf.String()

	expectedSubstrings := []string{
		"#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]",
		"pub enum Role {",
		"Unspecified = 0,",
		"Admin = 1,",
		"pub struct Address {",
		`#[prost(enumeration = "Role", tag = "2")]`,
		"pub role: i32,",
		`#[prost(enumeration = "account::Status", tag = "3")]`,
		"pub status: i32,",
		`#[prost(message, optional, tag = "4")]`,
		"pub billing_address: ::core::option::Option<Address>,",
		`#[prost(message, optional, tag = "5")]`,
		"pub profile: ::core::option::Option<account::Profile>,",
		`#[prost(message, repeated, tag = "6")]`,
		"pub extra_addresses: Vec<Address>,",
		"pub mod account {",
		"pub enum Status {",
		"pub struct Profile {",
		"pub home_address: ::core::option::Option<super::Address>,",
		`#[prost(enumeration = "super::account::Status", tag = "3")]`,
		"pub struct AccountRepository {",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code to contain %q, got:\n%s", exp, code)
		}
	}
	if strings.Contains(code, "AddressRepository") {
		t.Errorf("unannotated helper message Address must not generate AddressRepository, got:\n%s", code)
	}
}

func TestGenerateRustRepository_NonQueueVersionstampWithIndexesAndAtomics(t *testing.T) {
	logID := ir.FieldSpec{ProtoName: "log_id", GoName: "LogId", Number: 1, Kind: ir.KindBytes, IsVersionstamp: true}
	actor := ir.FieldSpec{ProtoName: "actor", GoName: "Actor", Number: 2, Kind: ir.KindString}
	tags := ir.FieldSpec{ProtoName: "tags", GoName: "Tags", Number: 3, Kind: ir.KindString, IsRepeated: true}
	views := ir.FieldSpec{ProtoName: "views", GoName: "Views", Number: 4, Kind: ir.KindInt64, Mutation: ir.MutationAdd}

	msg := ir.MessageSpec{
		Name:             "AuditLog",
		PrimaryKeyFields: []ir.FieldSpec{logID},
		Fields:           []ir.FieldSpec{logID, actor, tags, views},
		SecondaryIndexes: []ir.SecondaryIndexSpec{
			{
				Fields:  []ir.FieldSpec{actor},
				IndexID: 1001,
			},
			{
				Fields:      []ir.FieldSpec{tags},
				IsFanOut:    true,
				FanOutField: tags,
				IndexID:     1002,
			},
		},
	}

	var buf BufferPrinter
	GenerateMessage(&buf, msg)
	code := buf.String()

	expectedSubstrings := []string{
		"let user_version = self.store.next_user_version();",
		"let key = dir.pack_with_versionstamp(&(type_id, fdb_layer::DATA_NAMESPACE, fdb_layer::Versionstamp::incomplete(user_version)));",
		"let field_key = dir.pack_with_versionstamp(&(type_id, fdb_layer::FIELD_NAMESPACE, fdb_layer::Versionstamp::incomplete(user_version), 4i64));",
		"let idx_key = dir.pack_with_versionstamp(&(type_id, fdb_layer::INDEX_NAMESPACE, 1001i64, &entity.actor, fdb_layer::Versionstamp::incomplete(user_version)));",
		"let idx_key = dir.pack_with_versionstamp(&(type_id, fdb_layer::INDEX_NAMESPACE, 1002i64, item, fdb_layer::Versionstamp::incomplete(user_version)));",
		"entity.log_id = fdb_layer::bytes_to_versionstamp(pk).as_bytes().to_vec();",
		"if !entity.log_id.is_empty() && entity.log_id.len() != 12 {",
		"if entity.log_id.is_empty() {",
		"return self.create(tr, dir, entity).await;",
		"decoded.log_id = entity.log_id.clone();",
		"entity.log_id = pk.to_vec();",
		"begin_tpl.push(fdb_layer::Element::Versionstamp(fdb_layer::bytes_to_versionstamp(b.as_ref())));",
		"let mut unpacked_tpls: Vec<Vec<fdb_layer::Element<'_>>> = Vec::with_capacity(kvs.len());",
		"if let Some(vs) = unpacked_tpls.get(i).and_then(|t| t.get(4)).and_then(|e| e.as_versionstamp()) {",
	}

	for _, exp := range expectedSubstrings {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code for non-queue versionstamp message to contain %q, got:\n%s", exp, code)
		}
	}
}

func TestGenerateRustRepository_SafeParamIdentsAndKeywords(t *testing.T) {
	valPK := ir.FieldSpec{ProtoName: "val", GoName: "Val", RustName: "val", Number: 1, Kind: ir.KindString}
	selfField := ir.FieldSpec{ProtoName: "self", GoName: "Self", RustName: "r#self", Number: 2, Kind: ir.KindString}
	counter := ir.FieldSpec{ProtoName: "count", GoName: "Count", Number: 3, Kind: ir.KindInt64, Mutation: ir.MutationAdd}

	msg := ir.MessageSpec{
		Name:             "CollisionEntity",
		PrimaryKeyFields: []ir.FieldSpec{valPK},
		Fields:           []ir.FieldSpec{valPK, selfField, counter},
		SecondaryIndexes: []ir.SecondaryIndexSpec{
			{
				Fields:  []ir.FieldSpec{selfField},
				IndexID: 2001,
			},
		},
	}

	var buf BufferPrinter
	GenerateMessage(&buf, msg)
	code := buf.String()

	for _, exp := range []string{
		"self_: &str,",
		"val_param: &str,",
		"val: i64,",
	} {
		if !strings.Contains(code, exp) {
			t.Errorf("expected generated Rust code to contain %q, got:\n%s", exp, code)
		}
	}
	if strings.Contains(code, "r#self") {
		t.Errorf("generated Rust code must not contain invalid raw identifier r#self:\n%s", code)
	}
}
