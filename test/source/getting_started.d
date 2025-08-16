module testdataframe.gettingstarted;

import dataframe;

import std.stdio;

import numir;
import mir.ndslice;

import std.conv;
import std.typecons;
import std.sumtype;

void testGettingStarted()
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
