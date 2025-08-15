module dataframe.core;


import std.variant;
import std.sumtype;
import std.container;
import std.conv;

import mir.ndslice;

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
   this(string title = "") { this.title = title;}

   ~this() {
      foreach(i; 0..Ts.length) {
         if (colValues[i]) {
            destroy(colValues[i]);
         }
      }
   }

   void setTitle(string title) {this.title = title;}

   void setIndexName(string indexName) {this.indexName = indexName;}

   string name(size_t  index) const {return colNames[index];}

   string[Ts.length] names() const {return colNames;}

   // Set the column at index.
   void setcol(T)(size_t index, string name, Slice!(T*) values) {
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

   // Set the column at the static Index.
   void setcol(T, size_t Index)(string name, Slice!(T*) values) {
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
      //auto ptr = cast(Slice!(T*)*) colValues[index];
   }

   // Get the column at index
   Slice!(T*) col(T)(size_t index) const {
      alias ReturnType = Slice!(T*);
      auto ptr = cast(Slice!(T*)*) colValues[index];
      return *ptr;
   }

   // Get the column at static Index
   auto col(size_t Index)() const {
      alias T = Ts[Index];
      auto ptr = cast(Slice!(Ts[Index]*)*) colValues[Index];
      return *ptr;
   }

   // Create a new data frame by adding a column at the end.
   TypedDataFrame!(Ts, T) addcol(T)(string name, Slice!(T*) values) const {
      auto df = new TypedDataFrame!(Ts, T)();
      // Copy data
      static foreach(i; 0..Ts.length) {
         df.setcol(i, this.name(i), this.col!i());
      }
      // Copy new column
      df.setcol(Ts.length, name, values);
      return df;
   }

   // Create a new data frame by adding a column at index Index.
   auto addcol(T, size_t Index)(string name, Slice!(T*) values) const {
      assert(!this.hasColName(name));
      static if (Index == 0) {
         auto df = new TypedDataFrame!(T, Ts)();
      } else {
         auto df = new TypedDataFrame!(Ts[0..Index], T, Ts[Index..Ts.length])();
      }
      // Copy data
      static if (Index > 0) {
         static foreach(i; 0..Index) {
            df.setcol(i, this.name(i), this.col!i());
         }
      }
      // Copy new column
      df.setcol!T(Index, name, values);

      // Copy remaining data
      static if (Ts.length >= Index) {
         static foreach(i; Index..Ts.length) {
            df.setcol(i+1, this.name(i), this.col!(i)());
         }
      }
      return df;
   }

   override string toString(){
      string str = "";
      if (title.length >0) {
         str ~= title;
         str ~= "\n";
      }

      enum size_t cols = Ts.length;
      //size_t rows = this.col!0().elementCount;

      static foreach(j; 0..cols) {
         str ~= name(j);
         str ~= ": ";
         str ~= to!string(this.col!j());
         static if (j + 1 < cols) {
            str ~= "\n";
         }
      }
      return str;
   }
}

alias DataFrame(Ts...) = TypedDataFrame!Ts;


