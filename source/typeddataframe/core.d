module typeddataframe.core;


import std.variant;
import std.sumtype;
import std.container;
import std.conv;
import std.typecons;
import std.string;
import std.meta;
import std.algorithm: min, max;

import mir.ndslice;
import numir;

import std.stdio;

/++
Type of the column.
+/
enum Type{
   None,
   Int,
   Long,
   ULong,
   Float,
   Double,
   String
}

/++
   Get the column type
+/
Type getColumnType(T)() {
   return Type.None;
}

Type getColumnType(T : int)() {
   return Type.Int;
}

Type getColumnType(T : long)() {
   return Type.Long;
}

Type getColumnType(T : ulong)() {
   return Type.ULong;
}

Type getColumnType(T : float)() {
   return Type.Float;
}

Type getColumnType(T : double)() {
   return Type.Double;
}

Type getColumnType(T : string)() {
   return Type.String;
}

/++
   Column value type.
+/
alias ColumnValue = SumType!(Slice!(int*), Slice!(long*), Slice!(ulong*),
   Slice!(float*), Slice!(double*), Slice!(string*));

/++
   Cell value type.
+/
alias CellValue = SumType!(int, long, ulong, float, double, string);

/++
Data frame slice.
+/
class DataFrameSlice(Ts...){
private:
   TypedDataFrame!Ts data;
   size_t rowStart;
   size_t rowEnd;
   size_t colStart;
   size_t colEnd;

   Tuple!(ulong, ulong) getColumnIndices(ulong[] colIndices) {
      size_t colLength = colIndices.length;
      size_t colStart = colIndices[0];
      size_t colEnd = 0;
      if (colLength == 1) {
         colEnd = colIndices[0]+1;
      } else {
         colEnd = colIndices[1];
      }
      return tuple(colStart, colEnd);
   }

   Tuple!(ulong, ulong) getRowIndices(ulong[] rowIndices) {
      size_t rowLength = rowIndices.length;
      size_t rowStart = rowIndices[0];
      size_t rowEnd = 0;
      if (rowLength == 1) {
         rowEnd = rowStart + 1;
      } else {
         rowEnd = rowIndices[1];
      }
      return tuple(rowStart, rowEnd);
   }

public:
   this(TypedDataFrame!Ts df, ulong[] rows, ulong[] cols) {
      this.data = df;
      auto colIndices = getColumnIndices(cols);
      colStart = colIndices[0];
      colEnd = colIndices[1];
      auto rowIndices = getRowIndices(rows);
      rowStart = rowIndices[0];
      rowEnd = rowIndices[1];
   }

   // this = int
   void opAssign(int value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(int*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }
      }
   }

   // this = long
   void opAssign(long value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(long*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }
      }
   }

   // this = ulong
   void opAssign(ulong value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(ulong*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }

      }
   }

   // this = float
   void opAssign(float value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(float*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }
      }
   }

   // this = double
   void opAssign(double value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(double*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }

      }
   }

   // this = string
   void opAssign(string value) {
      for(size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(string*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = value;
         }

      }
   }

   // this = Slice!(int*)
   void opAssign(Slice!(int*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(int*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = Slice!(long*)
   void opAssign(Slice!(long*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(long*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = Slice!(ulong*)
   void opAssign(Slice!(ulong*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(ulong*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = Slice!(float*)
   void opAssign(Slice!(float*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(float*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = Slice!(double*)
   void opAssign(Slice!(double*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(double*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = Slice!(string*)
   void opAssign(Slice!(string*) values) {
      assert(values.elementCount == rowEnd - rowStart, "values should be a slice of size " ~ to!string(rowEnd - rowStart));

      for (size_t i = colStart; i < colEnd; i++) {
         alias SliceType = Slice!(string*);
         auto col = data.col(i).get!SliceType;
         for (size_t j = rowStart; j < rowEnd; j++) {
            col[j] = values[j - rowStart];
         }
      }
   }

   // this = tuple(Slice!Rs)
   void assign(Rs...)(Tuple!Rs values) {
      enum size_t cols = values.length;
      static foreach(i; 0..cols) {
         assert(values[i].elementCount == rowEnd - rowStart, "values[" ~ i.to!string ~ " should be a slice of size "
            ~ to!string(rowEnd - rowStart));
      }
      static foreach(i; 0..cols) {
         {
            alias SliceType = Rs[i];
            auto col = data.col(i + colStart).get!SliceType;
            for (size_t j = rowStart; j < rowEnd; j++) {
               col[j] = values[i][j - rowStart];
            }
         }
      }
   }

   // Get the slice
   Slice!(T*) slice(T)() {
      assert((colStart + 1 == colEnd) || (colStart + 1 != colEnd && rowStart + 1 == rowEnd),
         "DataFrameSlice must have only one row or one column to convert it to a Slice!(T*)");
      if (colStart + 1 == colEnd) {
         // colStart + 1 = colEnd
         alias SliceType = Slice!(T*);
         auto col = data.col(colStart).get!SliceType;
         return col;
      } else {
         // rowStart + 1 == rowEnd
         size_t cols = colEnd - colStart;
         auto col = empty!T(cols);
         for (size_t i = colStart; i < colEnd; i++) {
            col[i - colStart] = data[rowStart, i].get!T;
         }
         return col;
      }
   }

   override string toString() {
      string str = "DataFrameSlice[";

      str ~= rowStart.to!string;
      str ~= "..";
      str ~= rowEnd.to!string;
      str ~= ", ";
      str ~= colStart.to!string;
      str ~= "..";
      str ~= colEnd.to!string;
      str ~= "]\n";

      for(size_t j = colStart; j < colEnd; j++) {
         str ~= data.name(j);
         str ~= ": ";
         auto col = data.col(j);
         str ~= to!string(col);
         if (j + 1 < colEnd) {
           str ~= "\n";
         }
      }
      return str;
   }
}


/++
Typed data frames, data frames with type information.
+/
class TypedDataFrame(Ts...) {
private:
   string title = "";
   string indexName = "";
   string[Ts.length] colNames;
   Type[Ts.length] colTypes;
   void*[Ts.length] colValues;

private:
   bool hasColName(string name) const {
      bool value = false;
      foreach(i; 0..Ts.length) {
         if (colNames[i] == name) {
            value = true;
            break;
         }
      }
      return value;
   }


public:
   this(const(string) title = "") { this.title = title;}

   ~this() {
      foreach(i; 0..Ts.length) {
         if (colValues[i]) {
            destroy(colValues[i]);
         }
      }
   }

   // Return whether the data frame is empty
   bool empty() const {
      bool ret = false;
      foreach(type; colTypes) {
         if (type == Type.None) {
            ret = true;
            break;
         }
      }
      return ret;
   }

   // Get the number of columns
   size_t cols() const { return Ts.length;}

   // Get the number of rows
   size_t rows() const {
      alias T = Ts[0];
      alias SliceType = Slice!(T*);
      auto colPtr = colValues[0];
      auto col = cast(SliceType*) colPtr;
      return col.elementCount;
   }

   // Get the shape
   size_t[2] shape() const {
      return [rows(), cols()];
   }

   // Set the title
   void setTitle(string title) {this.title = title;}

   // Set the index name
   void setIndexName(string indexName) {this.indexName = indexName;}

   // Get the name at index
   string name(size_t  index) const {return colNames[index];}

   // Return the column names
   string[Ts.length] names() const {return colNames;}

   // Set the column at index.
   void setCol(T)(size_t index, string name, Slice!(T*) values) {
      assert(!hasColName(name));
      // Set the name
      colNames[index] = name;
      // Set the data
      Slice!(T*)* valuesPtr = new Slice!(T*)();
      *valuesPtr = values;
      colValues[index] = cast(void*) valuesPtr;
      // Set the column type
      colTypes[index] = getColumnType!T();
      //auto ptr = cast(Slice!(T*)*) colValues[index];
   }

   // Set the column at index from a sym type
   void setCol(T)(size_t index, string name, ColumnValue values) {
      assert(!hasColName(name));
      // Set the name
      colNames[index] = name;
      // Set the data
      Slice!(T*)* valuesPtr = new Slice!(T*)();
      *valuesPtr = values.get!(Slice!(T*));
      colValues[index] = cast(void*) valuesPtr;
      // Set the column type
      colTypes[index] = getColumnType!T();
   }

   // Set the column at the static Index.
   void setCol(T, size_t Index)(string name, Slice!(T*) values) {
      assert(T == Ts[Index]);
      assert(!hasColName(name));
      // Set the name
      colNames[Index] = name;
      // Set the data
      Slice!(T*)* valuesPtr = new Slice!(T*)();
      *valuesPtr = values;
      colValues[Index] = cast(void*) valuesPtr;
      // Set the column type
      colTypes[Index] = getColumnType!T();
   }

   // Get the column at index and convert it to Slice!(T*)
   Slice!(T*) col(T)(size_t index) const {
      //alias ReturnType = Slice!(T*);
      auto ptr = cast(Slice!(T*)*) colValues[index];
      return *ptr;
   }

   // Get the column at inde
   ColumnValue col(size_t index) const {
      auto type = colTypes[index];
      assert(type != Type.None, "Column at " ~ to!string(index) ~ " must have type.");
      if (type == Type.Int) {
         auto ptr = cast(Slice!(int*)*) colValues[index];
         return ColumnValue(*ptr);
      } else if (type == Type.Float) {
         auto ptr = cast(Slice!(float*)*) colValues[index];
         return ColumnValue(*ptr);
      } else if (type == Type.Double) {
         auto ptr = cast(Slice!(double*)*) colValues[index];
         return ColumnValue(*ptr);
      } else {
         auto ptr = cast(Slice!(string*)*) colValues[index];
         return ColumnValue(*ptr);
      }
   }

   // Create a new data frame by adding a column at the end.
   TypedDataFrame!(Ts, T) addCol(T)(string name, Slice!(T*) values) const {
      auto df = new TypedDataFrame!(Ts, T)();
      // Copy data
      static foreach(i; 0..Ts.length) {
         df.setCol!(Ts[i])(i, this.name(i), this.col(i));
      }
      // Copy new column
      df.setCol!T(Ts.length, name, values);
      return df;
   }

   // Create a new data frame by adding a column at index Index.
   auto addCol(size_t Index, T)(string name, Slice!(T*) values) const {
      assert(!this.hasColName(name));
      static if (Index == 0) {
         auto df = new TypedDataFrame!(T, Ts)();
      } else {
         auto df = new TypedDataFrame!(Ts[0..Index], T, Ts[Index..Ts.length])();
      }
      // Copy data
      static if (Index > 0) {
         static foreach(i; 0..Index) {
            df.setCol!(Ts[i])(i, this.name(i), this.col(i));
         }
      }
      // Copy new column
      df.setCol!T(Index, name, values);

      // Copy remaining data
      static if (Ts.length >= Index) {
         static foreach(i; Index..Ts.length) {
            df.setCol!(Ts[i])(i+1, this.name(i), this.col(i));
         }
      }
      return df;
   }

   // Get the column at index [index]
   ColumnValue opIndex(size_t index) const {
      return col(index);
   }

   // Get the column named name [name]
   ColumnValue opIndex(string name) const {
      size_t index = 0;
      foreach(i; 0..Ts.length) {
         if (colNames[i] == name) {
            index = i;
            break;
         }
      }
      return col(index);
   }

   // Overload .opIndex(i, j) or [i,j]
   CellValue opIndex(size_t i, size_t j) const {
      auto valuesPtr = colValues[j];
      auto type = colTypes[j];
      assert(type != Type.None);

      if (type == Type.Int) {
         auto values = cast(Slice!(int*)*) valuesPtr;
         return CellValue((*values)[i]);
      } else if (type == Type.Long) {
         auto values = cast(Slice!(long*)*) valuesPtr;
         return CellValue((*values)[i]);
      } else if (type == Type.ULong) {
         auto values = cast(Slice!(ulong*)*) valuesPtr;
         return CellValue((*values)[i]);
      } else if (type == Type.Float) {
         auto values = cast(Slice!(float*)*) valuesPtr;
         return CellValue((*values)[i]);
      } else if (type == Type.Double){
         auto values = cast(Slice!(double*)*) valuesPtr;
         return CellValue((*values)[i]);
      } else {
         auto values = cast(Slice!(string*)*) valuesPtr;
         return CellValue((*values)[i]);
      }
   }

   // Overload [[], ]
   DataFrameSlice!Ts opIndex(size_t[] i, size_t j) {
      DataFrameSlice!Ts df = new DataFrameSlice!Ts(this, i, [j]);
      return df;
   }

   // Overload [ , []]
   DataFrameSlice!Ts opIndex(size_t i, size_t[] j) {
      DataFrameSlice!Ts df = new DataFrameSlice!Ts(this, [i], j);
      return df;
   }

   DataFrameSlice!Ts opIndex(size_t[] i, size_t[] j) {
      DataFrameSlice!Ts df = new DataFrameSlice!Ts(this, i, j);
      return df;
   }

   size_t[2] opSlice(size_t dim)(size_t start, size_t end) {
      return [start, end];
   }

   // Head first 5 rows with corresponding rows
   DataFrameSlice!Ts head() {
      assert(!empty(), "Data frame must not be empty");
      size_t col = cols();
      size_t row = rows();
      return this[0..min(5, row), 0..col];
   }

   // Tail last 5 rows with corresponding columns
   DataFrameSlice!Ts tail() {
      assert(!empty(), "Data frame must not be empty");
      size_t col = cols();
      size_t row = rows();
      return this[max(0, row-5)..row, 0..col];
   }


   override string toString(){
      assert(!empty(), "Data frame must not be empty");

      string str = "DataFrame[";
      str ~= rows().to!string;
      str ~= "x";
      str ~= cols().to!string;
      str ~= "]\n";
      if (title.length >0) {
         str ~= title;
         str ~= "\n";
      }

      enum size_t cols = Ts.length;
      //size_t rows = this.col!0().elementCount;

      static foreach(j; 0..cols) {
         str ~= name(j);
         str ~= ": ";
         str ~= to!string(this.col(j));
         static if (j + 1 < cols) {
            str ~= "\n";
         }
      }
      return str;
   }
}

/++
DataFrame alias for TypedDataFrame
+/
alias DataFrame(Ts...) = TypedDataFrame!Ts;

/++
Get type string, Slice!(Ts[0]), string, Slice!(Ts[1]), ....
+/
template SliceTupleType(T, Ts...) {
   alias Type = AliasSeq!(string, Slice!(T*), SliceTupleType!Ts.Type);
}
template SliceTupleType(T){
   alias Type = AliasSeq!(string, Slice!(T*));
}

/++
   Create a new data frame from data.
   dataFrame("col1Name", col1, "col2Name", col2, ...)
+/
DataFrame!Ts dataFrame(Ts...)(SliceTupleType!Ts.Type values) {
   auto df = new DataFrame!Ts();
   enum size_t length = values.length;
   assert(length % 2 == 0);
   // Set the data
   static foreach(i;0..length) {
      static if (i%2 == 1) {
         {
            alias T = Ts[i/2];
            df.setCol!T(i/2, values[i-1], values[i]);
         }
      }
   }
   return df;
}

