// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

/// A Map is a logical nested type that is represented as
///
/// List<entry: Struct<key: K, value: V>>
///
/// In this layout, the keys and values are each respectively contiguous. We do
/// not constrain the key and value types, so the application is responsible
/// for ensuring that the keys are hashable and unique. Whether the keys are sorted
/// may be set in the metadata for this field
///
/// In a Field with Map type, the Field has a child Struct field, which then
/// has two children: key type and the second the value type. The names of the
/// child fields may be respectively "entry", "key", and "value", but this is
/// not enforced
///
/// Map
///   - child[0] entry: Struct
///     - child[0] key: K
///     - child[1] value: V
///
/// Neither the "entry" field nor the "key" field may be nullable.
///
/// The metadata is structured so that Arrow systems without special handling
/// for Map can make Map an alias for List. The "layout" attribute for the Map
/// field must have the same contents as a List.
internal struct Map : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static Map GetRootAsMap(ByteBuffer _bb) { return GetRootAsMap(_bb, new Map()); }
  public static Map GetRootAsMap(ByteBuffer _bb, Map obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public Map __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /// Set to true if the keys within each value are sorted
  public bool KeysSorted { get { int o = __p.__offset(4); return o != 0 ? 0!=__p.bb.Get(o + __p.bb_pos) : (bool)false; } }

  public static Offset<Map> CreateMap(FlatBufferBuilder builder,
      bool keysSorted = false) {
    builder.StartObject(1);
    Map.AddKeysSorted(builder, keysSorted);
    return Map.EndMap(builder);
  }

  public static void StartMap(FlatBufferBuilder builder) { builder.StartObject(1); }
  public static void AddKeysSorted(FlatBufferBuilder builder, bool keysSorted) { builder.AddBool(0, keysSorted, false); }
  public static Offset<Map> EndMap(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<Map>(o);
  }
};


}