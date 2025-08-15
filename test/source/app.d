import dataframe;

import std.stdio;

import numir;
import mir.ndslice;

void main()
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
      auto newdf = new DataFrame!(float, float)();
      newdf.setTitle("DataFrame using DataFrame");
      newdf.setCol(0, "a", x);
      newdf.setCol(1, "b", x);
      // set value
      newdf.setValue!float(0, 0, 99.0f);
      // Get value [i,j]
      float value = newdf.opIndex!float(0, 0);
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
   }

}
