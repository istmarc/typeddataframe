module testdataframe.basics;

import dataframe;

import std.stdio;

import numir;
import mir.ndslice;

import std.conv;
import std.typecons;
import std.sumtype;

void testBasics()
{
   auto df = new TypedDataFrame!(float, double, string)("DataFrame DF");
   df.setCol(0, "a", zeros!float(10));
   df.setCol(1, "b", ones!double(10));
   df.setCol(2, "c", ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"].sliced(10));

   auto col0 = df.col(0);
   writeln(col0);

   auto col1 = df.col!double(1);
   writeln(col1);

   auto col11 = df.col(1);
   writeln(col11);

   writeln(df);

   auto x = zeros!float(10);
   x[0] = 1.0f;
   foreach(i; 1..10) {
      x[i] = x[i-1] + 1.0f;
   }

   auto y = zeros!double(10);
   y[0] = 1.0;
   foreach(i; 1..10) {
      y[i] = y[i-1] + 1.0;
   }

   {
      writeln("Add column at the end");
      auto newdf = df.addCol!float("d", x);
      newdf.setTitle("DataFrame NEW DF");
      writeln(newdf);
   }

   {
      writeln("Add column at the begining");
      auto newdf = df.addCol!(0, float)("d", x);
      writeln(newdf);
   }

   {
      writeln("Add column in the middle");
      auto newdf = df.addCol!1("d", x);
      writeln(newdf);
   }

   {
      auto newdf = new DataFrame!(float, double)();
      newdf.setCol(0, "x", x);
      newdf.setCol(1, "y", y);
      // set value
      newdf.setValue!float(0, 0, 99.0f);
      // Get value [i,j]
      float value = newdf[0, 0].get!float;
      writeln("value[0,0] = ", value);
      writeln(newdf);
      writeln("Get the first column");
      writeln(newdf.opIndex!float(0));

      {
         writeln("Slices [0, 0...2]");
         auto slice = newdf[0, 0..2];
         writeln(slice);
      }

      {
         writeln("SLices [0..10, 0]");
         auto slice = newdf[0..10, 0];
         writeln(slice);
      }

      {
         writeln("Slices [0..10, 0..2]");
         auto slice = newdf[0..10, 0..2];
         writeln(slice);
      }

      {
         writeln("Assign value to slices");
         auto slice = newdf[0..10, 0];
         slice = 99.0f;
         writeln("slice: ", slice);
         writeln("newdf: ", newdf);
      }

      {
         writeln("Assign a mir slice to a slice");
         writeln("Before asignment");
         writeln(newdf);
         auto slice = newdf[0..10, 0];
         slice = zeros!float(10);
         writeln("After signment");
         writeln(slice);
         writeln(newdf);
      }

      {
         writeln("Assign a list of mir slices to a slice");
         auto slice = newdf[0..10, 0..2];
         writeln(slice);
         writeln("After asignement");
         auto arr = tuple(ones!float(10), empty!double(10));
         slice.assign(arr);
         writeln(slice);
      }

      {
         writeln("Convert a data frame slice to a slice");
         writeln(df);
         newdf.setTitle("New DataFrame");
         writeln(newdf);
         auto dfslice = df[0..10, 0];
         auto newdfslice = newdf[0..10, 0].slice!float;
         writeln("slice = ", newdfslice);
         dfslice = newdfslice;
         writeln(dfslice);
      }

      {
         writeln("Convert a data frame slice row to a slice");
         auto d = new DataFrame!(float, float)("DF");
         d.setCol(0, "x", empty!float(5));
         d.setCol(1, "y", empty!float(5));
         writeln(d);
         writeln(d[2, 0..2].slice!float);
      }
   }

}
