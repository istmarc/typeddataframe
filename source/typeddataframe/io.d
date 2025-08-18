module typeddataframe.io;

import typeddataframe.core: DataFrame;

import std.stdio: File;
import std.csv;
import std.typecons: Tuple;
import std.conv;
import std.array;
import std.algorithm: joiner;
import std.sumtype: get;

import mir.ndslice: Slice;
import numir: empty;

// Read from csv
auto readCsv(IndexType, Ts...)(string fileName) {
   auto df = new DataFrame!(IndexType, Ts)();
   auto file = File(fileName, "r");
   enum size_t cols = Ts.length;
   // How to read the first line of a csv fil
   auto names = file.byLine.front.split(",");

   auto records = csvReader!(Tuple!(IndexType, Ts))(file.byLine.joiner("\n"), null);
   size_t rows = 1;
   foreach(record; records) {
      rows++;
   }
   // Initialize data with empty
   static foreach(i; 0..cols) {
      {
         alias T = Ts[i];
         df.setCol(i, names[i+1].to!string, empty!T(rows));
      }
   }
   df.setIndex(empty!IndexType(rows));
   file.close();
   auto newFile = File(fileName, "r");
   auto newRecords = csvReader!(Tuple!(IndexType, Ts))(newFile.byLine.joiner("\n"), null);
   // Set the values
   size_t row = 0;
   foreach(record; newRecords) {
      // Set the data
      static foreach(i; 0..cols) {
         {
            alias T = Ts[i];
            alias SliceType = Slice!(T*);
            auto col = df.col(i).get!SliceType;
            col[row] = record[i+1];
         }
      }
      // TODO Set the index
      df.setIndex(row, record[0]);
      row++;
   }

   return df;
}

// Save to csv

