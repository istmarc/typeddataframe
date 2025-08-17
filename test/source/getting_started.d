module testdataframe.gettingstarted;

import typeddataframe;

import std.stdio;

import numir;
import mir.ndslice;

import std.conv;
import std.typecons;
import std.sumtype;

void testGettingStarted()
{
   {
      size_t n = 6;
      auto x = zeros!float(n);
      x[0] = 1.0f;
      foreach(i; 1..n) {
         x[i] = x[i-1] + 1.0f;
      }

      auto y = zeros!double(n);
      y[0] = 1.0;
      foreach(i; 1..n) {
         y[i] = y[i-1] + 1.0;
      }

      auto z = ["a", "b", "c", "d", "e", "f"].sliced(n);

      auto df = dataFrame!(float, double, string)("x", x, "y", y, "z", z);
      writeln(df.head());
      writeln(df.tail());

      // Access columns
      writeln(df["x"]);
   }

   {
      auto df = dataFrame!(float, double, string)("x", empty!float(4), "y", ones!double(4), "z", ["a", "b", "c", "d"].sliced(4));
      writeln(df);
      // Acees column
      writeln(df["z"]);
      // Access row
      writeln(df[0, 0..3]);
      // Acces column at index
      writeln(df[0]);
      // Colum names
      writeln(df.names());
   }

   {
      auto df = new DataFrame!(long, string)();
      df.setCol(0, "A", [1.0, 2.0].sliced(2));
      df.setCol(1, "B", ["X", "Y"].sliced(2));
      writeln(df);
      // Get the number of rows
      writeln(df.rows());
      // Get the number of columns
      writeln(df.cols());
      // Get the shape
      writeln(df.shape());
   }

   {
      auto df = dataFrame!(float, double, string)("x", empty!float(3), "y", empty!double(3), "z", ["A", "B", "C"].sliced(3));
      writeln(df);
      // Get the column at index
      writeln(df[0]);
      // Get the column from its name
      writeln(df["z"]);
      // Slicing
      auto slice = df[0..3, 0];
      // Assign a value
      slice = 1.0f;
      writeln(df);
      // Asignment a mir slice
      slice = [1.0f, 2.0f, 3.0f].sliced(3);
      writeln(df);
   }

   {
      auto df = dataFrame!(float, float)("x", ones!float(3), "y", zeros!float(3));
      writeln(df);
      // Slicing
      auto slice = df[0..3, 0..2].matrix!float;
      writeln(slice);
   }

}
