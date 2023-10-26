// <auto-generated>
//  automatically generated by the FlatBuffers compiler, do not modify
// </auto-generated>

namespace Apache.Arrow.Flatbuf
{

using global::System;
using global::FlatBuffers;

internal struct Int : IFlatbufferObject
{
  private Table __p;
  public ByteBuffer ByteBuffer { get { return __p.bb; } }
  public static Int GetRootAsInt(ByteBuffer _bb) { return GetRootAsInt(_bb, new Int()); }
  public static Int GetRootAsInt(ByteBuffer _bb, Int obj) { return (obj.__assign(_bb.GetInt(_bb.Position) + _bb.Position, _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __p.bb_pos = _i; __p.bb = _bb; }
  public Int __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int BitWidth { get { int o = __p.__offset(4); return o != 0 ? __p.bb.GetInt(o + __p.bb_pos) : (int)0; } }
  public bool IsSigned { get { int o = __p.__offset(6); return o != 0 ? 0!=__p.bb.Get(o + __p.bb_pos) : (bool)false; } }

  public static Offset<Int> CreateInt(FlatBufferBuilder builder,
      int bitWidth = 0,
      bool is_signed = false) {
    builder.StartObject(2);
    Int.AddBitWidth(builder, bitWidth);
    Int.AddIsSigned(builder, is_signed);
    return Int.EndInt(builder);
  }

  public static void StartInt(FlatBufferBuilder builder) { builder.StartObject(2); }
  public static void AddBitWidth(FlatBufferBuilder builder, int bitWidth) { builder.AddInt(0, bitWidth, 0); }
  public static void AddIsSigned(FlatBufferBuilder builder, bool isSigned) { builder.AddBool(1, isSigned, false); }
  public static Offset<Int> EndInt(FlatBufferBuilder builder) {
    int o = builder.EndObject();
    return new Offset<Int>(o);
  }
};


}
