module testdataframe.creation;

import typeddataframe;

import std.stdio;

import numir;
import mir.ndslice;

import std.conv;
import std.typecons;
import std.sumtype;

void testCreation()
{
   auto x = zeros!float(5);
   x[0] = 1.0f;
   foreach(i; 1..5) {
      x[i] = x[i-1] + 1.0f;
   }

   auto y = zeros!double(5);
   y[0] = 1.0;
   foreach(i; 1..5) {
      y[i] = y[i-1] + 1.0;
   }

   auto z = ["a", "b", "c", "d", "e"].sliced(5);

   auto df = dataFrame!(ulong, float, double, string)("x", x, "y", y, "z", z);
   writeln(df);

}
