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
class DataFrameSlice(IndexType, Ts...){
private:
   TypedDataFrame!(IndexType, Ts) data;
   ulong[] selectedCols;
   ulong[] selectedRows;

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
   this(TypedDataFrame!(IndexType, Ts) df, ulong[] rows, ulong[] cols, bool fromSlice) {
      data = df;
      if (fromSlice) {
         if (rows.length == 2 || rows.length == 1) {
            auto rowIndices = getRowIndices(rows);
            size_t rowStart = rowIndices[0];
            size_t rowEnd = rowIndices[1];
            assert(rowEnd > rowStart);
            selectedRows.length = rowEnd - rowStart;
            for(size_t i = rowStart; i < rowEnd; i++) {
               selectedRows[i-rowStart] = i;
            }
         } else {
            selectedRows = rows;
         }
         if (cols.length == 2 || cols.length == 2) {
            auto colIndices = getColumnIndices(cols);
            size_t colStart = colIndices[0];
            size_t colEnd = colIndices[1];
            selectedCols.length = colEnd - colStart;
            for(size_t i = colStart; i <colEnd; i++) {
               selectedCols[i - colStart] = i;
            }
         } else {
            selectedCols = cols;
         }
      } else {
         selectedRows = rows;
         selectedCols = cols;
      }
   }

   // this = int
   void opAssign(int value) {
      foreach(i; selectedCols) {
         alias SliceType = Slice!(int*);
         auto col = data.col(i).get!SliceType;
         foreach(j; selectedRows) {
            col[j] = value;
         }
      }
   }

   // this = long
   void opAssign(long value) {
      foreach(i; selectedCols) {
         alias SliceType = Slice!(long*);
         auto col = data.col(i).get!SliceType;
         foreach(j; selectedRows) {
            col[j] = value;
         }
      }
   }

   // this = ulong
   void opAssign(ulong value) {
      foreach(i; selectedCols) {
         alias SliceType = Slice!(ulong*);
         auto col = data.col(i).get!SliceType;
         foreach (j; selectedRows) {
            col[j] = value;
         }

      }
   }

   // this = float
   void opAssign(float value) {
      writeln("selected ROws = ", selectedRows);
      foreach(i; selectedCols) {
         alias SliceType = Slice!(float*);
         auto col = data.col(i).get!SliceType;
         foreach (j; selectedRows) {
            col[j] = value;
         }
      }
   }

   // this = double
   void opAssign(double value) {
      foreach(i; selectedCols) {
         alias SliceType = Slice!(double*);
         auto col = data.col(i).get!SliceType;
         foreach (j; selectedRows) {
            col[j] = value;
         }

      }
   }

   // this = string
   void opAssign(string value) {
      foreach(i; selectedCols) {
         alias SliceType = Slice!(string*);
         auto col = data.col(i).get!SliceType;
         foreach (j; selectedRows) {
            col[j] = value;
         }

      }
   }

   // this = Slice!(int*)
   void opAssign(Slice!(int*) values) {
      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(int*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }
   }

   // this = Slice!(long*)
   void opAssign(Slice!(long*) values) {
      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(long*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }

   }

   // this = Slice!(ulong*)
   void opAssign(Slice!(ulong*) values) {
      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(ulong*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }

   }

   // this = Slice!(float*)
   void opAssign(Slice!(float*) values) {

      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(float*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }

   }

   // this = Slice!(double*)
   void opAssign(Slice!(double*) values) {

      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(double*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }
   }

   // this = Slice!(string*)
   void opAssign(Slice!(string*) values) {
      assert(values.elementCount == selectedRows.length,
         "values should be a slice of size " ~ to!string(selectedRows.length));

      foreach (i; selectedCols) {
         alias SliceType = Slice!(string*);
         auto col = data.col(i).get!SliceType;
         size_t k = 0;
         foreach (j; selectedRows) {
            col[j] = values[k];
            k++;
         }
      }
   }

   // this = tuple(Slice!Rs)
   void assign(Rs...)(Tuple!Rs values) {
      enum size_t cols = values.length;
      static foreach(i; 0..cols) {
         assert(values[i].elementCount == selectedRows.length, "values[" ~ i.to!string ~ " should be a slice of size "
            ~ to!string(selectedRows.length));
      }

      static foreach(i; 0..cols) {
         {
            alias SliceType = Rs[i];
            auto col = data.col(i).get!SliceType;
            size_t k = 0;
            foreach(j; selectedRows) {
               col[j] = values[i][k];
               k++;
            }
         }
      }
   }

   // Convert to a slice
   Slice!(T*) slice(T)() {
      size_t cols = selectedCols.length;
      size_t rows = selectedRows.length;
      alias SliceType = Slice!(T*);
      auto slice = empty!T(cols*rows);
      size_t k = 0;
      foreach(i; 0..selectedCols.length) {
         auto col = data.col(i).get!SliceType;
         foreach(j; selectedRows) {
            slice[k] = col[j];
         }
         k++;
      }
      return slice;
   }


   // Convert to a matrix
   Slice!(T*, 2) matrix(T)() {
      alias SliceType = Slice!(T*, 2);
      auto mat = empty!T([selectedRows.length, selectedCols.length]);
      size_t k = 0;
      foreach(i; selectedRows) {
         foreach(j; selectedCols) {
            mat.iterator[k] = data[i, j].get!T;
            k++;
         }
      }
      return mat;
   }

   override string toString() {
      string str = "DataFrameSlice[";
      str ~= selectedRows.length.to!string;
      str ~= "x";
      str ~= selectedCols.length.to!string;
      str ~= "]\n";

      foreach(j; 0..selectedCols.length) {
         str ~= data.name(selectedCols[j]);
         str ~= ": ";
         auto col = data.col(selectedCols[j]);
         str ~= to!string(col);
         if (j + 1 < selectedCols.length) {
           str ~= "\n";
         }
      }
      return str;
   }
}


/++
Typed data frames, data frames with type information.
+/
class TypedDataFrame(IndexType, Ts...) {
private:
   string title = "";
   string indexName = "";
   string[Ts.length] colNames;
   Type[Ts.length] colTypes;
   void*[Ts.length] colValues;
   Slice!(IndexType*) index;

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

   // Return the index of the column name
   long indexColName(string name) const {
      long idx = -1;
      foreach(i; 0..Ts.length) {
         if (colNames[i] == name) {
            idx = i;
            break;
         }
      }
      return idx;
   }

public:
   this(const(string) title = "") {
      this.title = title;
   }

   this(Slice!(IndexType*) index, const(string) title="") {
      this.index = index;
   }

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

   // Fill index with 0..rows
   void fillIndex() {
      static if (__traits(isSame, IndexType, string)) {
         size_t row = this.rows();
         size_t value = 0;
         this.index = numir.empty!IndexType(row);
         foreach(i; 0..row) {
            index[i] = value.to!string;
            value++;
         }
      } else {
         size_t row = this.rows();
         this.index = numir.empty!IndexType(row);
         index[0] = cast(IndexType) 0;
         foreach(i; 1..row) {
            index[i] = index[i-1] + IndexType(1);
         }
      }
   }

   // Set the ith index to value
   void setIndex(size_t i, IndexType value) {
      index[i] = value;
   }

   void setIndex(Slice!(IndexType*) values) {
      index = values;
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
   TypedDataFrame!(IndexType, Ts, T) addCol(T)(string name, Slice!(T*) values) const {
      auto df = new TypedDataFrame!(IndexType, Ts, T)();
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
         auto df = new TypedDataFrame!(IndexType, T, Ts)();
      } else {
         auto df = new TypedDataFrame!(IndexType, Ts[0..Index], T, Ts[Index..Ts.length])();
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

   // Remove a column at Index, return a new data frame
   auto removeCol(size_t Index)() {
      static if (Index == 0) {
         auto df = new TypedDataFrame!(IndexType, Ts[1..Ts.length])();
      } else {
         auto df = new TypedDataFrame!(IndexType, Ts[0..Index], Ts[Index+1..Ts.length])();
      }
      // Copy data
      static if (Index == 0) {
         static foreach(i; 1..Ts.length){
            df.setCol!(Ts[i])(i-1, this.name(i), this.col(i));
         }
      }
      static if (Index > 0) {
         static foreach(i; 0..Index) {
            df.setCol!(Ts[i])(i, this.name(i), this.col(i));
         }
      }
      static if (Index > 0 && Index < Ts.length) {
         static foreach(i; Index+1..Ts.length) {
            df.setCol!(Ts[i])(i-1, this.name(i-1), this.col(i-1));
         }
      }
      // Copy index
      df.setIndex(index);
      return df;
   }

   // Remove a column at index, return a DataFrameSlice
   DataFrameSlice!(IndexType, Ts) removeCol(size_t index) {
      assert(index >= 0 && index < cols(), "Invalid index");
      ulong[] rowIndices;
      rowIndices.length = rows();
      foreach(i; 0..rowIndices.length) {
         rowIndices[i] = i;
      }
      ulong[] colIndices;
      colIndices.length = cols() - 1;
      size_t k = 0;
      foreach(i; 0..cols()) {
         if (i != index) {
            colIndices[k] = i;
            k++;
         }
      }
      auto df = new DataFrameSlice!(IndexType, Ts)(this, rowIndices, colIndices, false);
      return df;
   }

   DataFrameSlice!(IndexType, Ts) removeCol(string name) {
      size_t idx = indexColName(name);
      assert(idx != -1, "Invalid column name.");
      return removeCol(idx);
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
   DataFrameSlice!(IndexType, Ts) opIndex(size_t[] i, size_t j) {
      DataFrameSlice!(IndexType, Ts) df = new DataFrameSlice!(IndexType, Ts)(this, i, [j], true);
      return df;
   }

   // Overload [ , []]
   DataFrameSlice!(IndexType, Ts) opIndex(size_t i, size_t[] j) {
      DataFrameSlice!(IndexType, Ts) df = new DataFrameSlice!(IndexType, Ts)(this, [i], j, true);
      return df;
   }

   // Overload [[], []]
   DataFrameSlice!(IndexType, Ts) opIndex(size_t[] i, size_t[] j) {
      DataFrameSlice!(IndexType, Ts) df = new DataFrameSlice!(IndexType, Ts)(this, i, j, true);
      return df;
   }

   size_t[2] opSlice(size_t dim)(size_t start, size_t end) {
      return [start, end];
   }

   // Select rows and columns
   DataFrameSlice!(IndexType, Ts) select(ulong[] rows, ulong[] cols) {
      DataFrameSlice!(IndexType, Ts) df = new DataFrameSlice!(IndexType, Ts)(this, rows, cols, false);
      return df;
   }

   // Select rows and columns using names
   DataFrameSlice!(IndexType, Ts) select(ulong[] rows, string[] names) {
      ulong[] cols;
      cols.length = names.length;
      for(size_t i = 0; i < names.length; i++) {
         long idx = indexColName(names[i]);
         assert(idx != -1);
         cols[i] = cast(ulong) idx;
      }
      return select(rows, cols);
   }

   // Head first 5 rows with corresponding rows
   DataFrameSlice!(IndexType, Ts) head() {
      assert(!empty(), "Data frame must not be empty");
      size_t col = cols();
      size_t row = rows();
      return this[0..min(5, row), 0..col];
   }

   // Tail last 5 rows with corresponding columns
   DataFrameSlice!(IndexType, Ts) tail() {
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
      str ~= "index: ";
      str ~= index.to!string;
      str ~= "\n";

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
alias DataFrame(IndexType, Ts...) = TypedDataFrame!(IndexType, Ts);

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
DataFrame!(IndexType, Ts) dataFrame(IndexType, Ts...)(SliceTupleType!Ts.Type values) {
   auto df = new DataFrame!(IndexType, Ts)();
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
   df.fillIndex();
   return df;
}

/++
   Create a new data frame from data and index.
   dataFrame("col1Name", col1, "col2Name", col2, ..., index)
+/
DataFrame!(IndexType, Ts) dataFrame(IndexType, Ts...)(SliceTupleType!Ts.Type values, Slice!(IndexType*) index) {
   auto df = new DataFrame!(IndexType, Ts)();
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
   df.setIndex(index);
   return df;
}


