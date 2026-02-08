defmodule Spark.Connect.DataType.Boolean do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Boolean",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Byte do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Byte",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Short do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Short",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Integer do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Integer",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Long do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Long",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Float do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Float",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Double do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Double",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.String do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.String",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
  field(:collation, 2, type: :string)
end

defmodule Spark.Connect.DataType.Binary do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Binary",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.NULL do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.NULL",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Timestamp do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Timestamp",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Date do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Date",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.TimestampNTZ do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.TimestampNTZ",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Time do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Time",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:precision, 1, proto3_optional: true, type: :int32)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.CalendarInterval do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.CalendarInterval",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.YearMonthInterval do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.YearMonthInterval",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:start_field, 1, proto3_optional: true, type: :int32, json_name: "startField")
  field(:end_field, 2, proto3_optional: true, type: :int32, json_name: "endField")
  field(:type_variation_reference, 3, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.DayTimeInterval do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.DayTimeInterval",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:start_field, 1, proto3_optional: true, type: :int32, json_name: "startField")
  field(:end_field, 2, proto3_optional: true, type: :int32, json_name: "endField")
  field(:type_variation_reference, 3, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Char do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Char",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:length, 1, type: :int32)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.VarChar do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.VarChar",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:length, 1, type: :int32)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Decimal do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Decimal",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:scale, 1, proto3_optional: true, type: :int32)
  field(:precision, 2, proto3_optional: true, type: :int32)
  field(:type_variation_reference, 3, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.StructField do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.StructField",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:name, 1, type: :string)
  field(:data_type, 2, type: Spark.Connect.DataType, json_name: "dataType")
  field(:nullable, 3, type: :bool)
  field(:metadata, 4, proto3_optional: true, type: :string)
end

defmodule Spark.Connect.DataType.Struct do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Struct",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:fields, 1, repeated: true, type: Spark.Connect.DataType.StructField)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Array do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Array",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:element_type, 1, type: Spark.Connect.DataType, json_name: "elementType")
  field(:contains_null, 2, type: :bool, json_name: "containsNull")
  field(:type_variation_reference, 3, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Map do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Map",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:key_type, 1, type: Spark.Connect.DataType, json_name: "keyType")
  field(:value_type, 2, type: Spark.Connect.DataType, json_name: "valueType")
  field(:value_contains_null, 3, type: :bool, json_name: "valueContainsNull")
  field(:type_variation_reference, 4, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Geometry do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Geometry",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:srid, 1, type: :int32)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Geography do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Geography",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:srid, 1, type: :int32)
  field(:type_variation_reference, 2, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.Variant do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Variant",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type_variation_reference, 1, type: :uint32, json_name: "typeVariationReference")
end

defmodule Spark.Connect.DataType.UDT do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.UDT",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:type, 1, type: :string)
  field(:jvm_class, 2, proto3_optional: true, type: :string, json_name: "jvmClass")
  field(:python_class, 3, proto3_optional: true, type: :string, json_name: "pythonClass")

  field(:serialized_python_class, 4,
    proto3_optional: true,
    type: :string,
    json_name: "serializedPythonClass"
  )

  field(:sql_type, 5, proto3_optional: true, type: Spark.Connect.DataType, json_name: "sqlType")
end

defmodule Spark.Connect.DataType.Unparsed do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType.Unparsed",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field(:data_type_string, 1, type: :string, json_name: "dataTypeString")
end

defmodule Spark.Connect.DataType do
  @moduledoc false

  use Protobuf,
    full_name: "spark.connect.DataType",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof(:kind, 0)

  field(:null, 1, type: Spark.Connect.DataType.NULL, oneof: 0)
  field(:binary, 2, type: Spark.Connect.DataType.Binary, oneof: 0)
  field(:boolean, 3, type: Spark.Connect.DataType.Boolean, oneof: 0)
  field(:byte, 4, type: Spark.Connect.DataType.Byte, oneof: 0)
  field(:short, 5, type: Spark.Connect.DataType.Short, oneof: 0)
  field(:integer, 6, type: Spark.Connect.DataType.Integer, oneof: 0)
  field(:long, 7, type: Spark.Connect.DataType.Long, oneof: 0)
  field(:float, 8, type: Spark.Connect.DataType.Float, oneof: 0)
  field(:double, 9, type: Spark.Connect.DataType.Double, oneof: 0)
  field(:decimal, 10, type: Spark.Connect.DataType.Decimal, oneof: 0)
  field(:string, 11, type: Spark.Connect.DataType.String, oneof: 0)
  field(:char, 12, type: Spark.Connect.DataType.Char, oneof: 0)
  field(:var_char, 13, type: Spark.Connect.DataType.VarChar, json_name: "varChar", oneof: 0)
  field(:date, 14, type: Spark.Connect.DataType.Date, oneof: 0)
  field(:timestamp, 15, type: Spark.Connect.DataType.Timestamp, oneof: 0)

  field(:timestamp_ntz, 16,
    type: Spark.Connect.DataType.TimestampNTZ,
    json_name: "timestampNtz",
    oneof: 0
  )

  field(:calendar_interval, 17,
    type: Spark.Connect.DataType.CalendarInterval,
    json_name: "calendarInterval",
    oneof: 0
  )

  field(:year_month_interval, 18,
    type: Spark.Connect.DataType.YearMonthInterval,
    json_name: "yearMonthInterval",
    oneof: 0
  )

  field(:day_time_interval, 19,
    type: Spark.Connect.DataType.DayTimeInterval,
    json_name: "dayTimeInterval",
    oneof: 0
  )

  field(:array, 20, type: Spark.Connect.DataType.Array, oneof: 0)
  field(:struct, 21, type: Spark.Connect.DataType.Struct, oneof: 0)
  field(:map, 22, type: Spark.Connect.DataType.Map, oneof: 0)
  field(:variant, 25, type: Spark.Connect.DataType.Variant, oneof: 0)
  field(:udt, 23, type: Spark.Connect.DataType.UDT, oneof: 0)
  field(:geometry, 26, type: Spark.Connect.DataType.Geometry, oneof: 0)
  field(:geography, 27, type: Spark.Connect.DataType.Geography, oneof: 0)
  field(:unparsed, 24, type: Spark.Connect.DataType.Unparsed, oneof: 0)
  field(:time, 28, type: Spark.Connect.DataType.Time, oneof: 0)
end
